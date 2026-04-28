"""
台股日當沖即時系統 - daytrade_live.py
======================================
資料來源：永豐金 Shioaji API（Tick 推送，無延遲）
策略：五多五空（與 taiwan_daytrade_screener.py 相同邏輯）
狀態記憶：記憶體 dict（當日有效，程式關閉自動清空）

需求：
  pip install shioaji pandas

使用方式：
  # 基本啟動（監控昨日選股 CSV）
  python daytrade_live.py --csv 五策略選股_20260416.csv

  # 指定股票清單
  python daytrade_live.py --codes 2330,6209,2327

  # 設定命中門檻
  python daytrade_live.py --codes 2330,2327 --min-hit 2

  # 停損設定（%）
  python daytrade_live.py --csv xxx.csv --stop-loss 1.5

環境變數（必填）：
  SJ_API_KEY      永豐金 API Key
  SJ_SECRET_KEY   永豐金 Secret Key

架構說明：
  - Shioaji 訂閱 Tick，有成交才推送，不用 polling
  - 技術指標基準在啟動時用永豐金 kbars 昨日收盤資料預算一次，盤中固定不變
  - 歷史資料源：永豐金 kbars（主）→ TWSE/TPEX 官方 API（備援，免帳號）
  - 爆量判斷：用昨日收盤量 / 5日均量，不受盤中累積量影響
  - 當日部位追蹤：記憶體 dict，記錄進場價、停損價、目前損益
"""

import os
import sys
import io
import json
import math
import time
import asyncio
import datetime
import argparse
import threading
import unicodedata
import subprocess
import contextlib
from collections import defaultdict

import pandas as pd

try:
    import websockets
    HAS_WS = True
except ImportError:
    HAS_WS = False

# ──────────────────────────────────────────────
# 常數
# ──────────────────────────────────────────────
RSI_PERIOD   = 14
MACD_FAST    = 12
MACD_SLOW    = 26
MACD_SIGNAL  = 9
VOL_MULT     = 1.5
MIN_AVG_VOL  = 3000   # 5日均量最低門檻（張）
DEFAULT_STOP = 1.5    # 預設停損（%）

TRADE_START = datetime.time(9, 0)
TRADE_END   = datetime.time(13, 30)

# ──────────────────────────────────────────────
# Windows 通知（多層 fallback）
# ──────────────────────────────────────────────
_NOTIFY_ENABLED  = True          # --no-notify 可關閉
_SOUND_ENABLED   = True          # --no-sound 可關閉
_alert_state: dict = {}          # code → frozenset（上次發警報時的訊號集合），None 表示未達門檻
_notified_stops: set = set()     # 已通知過停損的代號
_notified_limits: set = set()    # 已通知過漲停/跌停出場的代號

def _send_notification(title: str, body: str):
    """
    依序嘗試三種通知方式：
      1. win10toast（pip install win10toast）
      2. winotify  （pip install winotify）
      3. PowerShell BalloonTip（Windows 內建，免安裝）
    在背景執行緒執行，不阻塞主迴圈。
    """
    def _run():
        # ── 聲音提示（winsound，Windows 內建，最可靠）──
        if _SOUND_ENABLED:
            try:
                import winsound
                winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
            except Exception:
                pass

        # ── 視覺通知 ──────────────────────────────────
        sent = False

        # 方法一：winotify（穩定，無 _show_toast 執行緒問題）
        if not sent:
            try:
                from winotify import Notification
                Notification(app_id="台股日當沖", title=title, msg=body).show()
                sent = True
            except Exception:
                pass

        # 方法二：PowerShell BalloonTip（免安裝 fallback）
        if not sent:
            try:
                t_safe = title.replace('"', '').replace("'", "")
                b_safe = body.replace('"',  '').replace("'", "")
                ps = (
                    "Add-Type -AssemblyName System.Windows.Forms; "
                    "$n = New-Object System.Windows.Forms.NotifyIcon; "
                    "$n.Icon = [System.Drawing.SystemIcons]::Information; "
                    f'$n.BalloonTipTitle = "{t_safe}"; '
                    f'$n.BalloonTipText  = "{b_safe}"; '
                    "$n.Visible = $true; "
                    "$n.ShowBalloonTip(6000); "
                    "Start-Sleep -Seconds 7; "
                    "$n.Dispose()"
                )
                subprocess.Popen(
                    ["powershell", "-WindowStyle", "Hidden", "-Command", ps],
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000),
                )
            except Exception:
                pass

    threading.Thread(target=_run, daemon=True).start()


def notify_signal(code: str, name: str, direction: str,
                  price: float, active_sigs: frozenset):
    """訊號出現或新增時發送通知"""
    if not _NOTIFY_ENABLED:
        return
    sig_str = " ".join(sorted(active_sigs))
    title   = f"📈 {code} {name}  {direction}方訊號"
    body    = f"現價 {price:.2f}　訊號：{sig_str}"
    _send_notification(title, body)


def notify_stop(code: str, name: str, direction: str,
                entry: float, price: float):
    """停損觸發時發送通知"""
    if not _NOTIFY_ENABLED:
        return
    if code in _notified_stops:
        return
    _notified_stops.add(code)
    pnl = (price - entry) / entry * 100 if direction == "多" \
          else (entry - price) / entry * 100
    title = f"🛑 {code} {name}  停損觸發"
    body  = f"進場 {entry:.2f} → 現價 {price:.2f}　損益 {pnl:+.2f}%"
    _send_notification(title, body)


# ──────────────────────────────────────────────
# 漲停 / 跌停價計算（台股最小升降單位）
# ──────────────────────────────────────────────
def _tick_size(price: float) -> float:
    """台股最小升降單位（tick）"""
    if price <   10: return 0.01
    if price <   50: return 0.05
    if price <  100: return 0.10
    if price <  500: return 0.50
    if price < 1000: return 1.00
    return 5.00

def calc_limit_up(prev_close: float) -> float:
    """漲停價：前收 × 1.10，無條件捨去到最小升降單位"""
    raw  = prev_close * 1.10
    tick = _tick_size(raw)
    return round(math.floor(raw / tick) * tick, 2)

def calc_limit_down(prev_close: float) -> float:
    """跌停價：前收 × 0.90，無條件進位到最小升降單位"""
    raw  = prev_close * 0.90
    tick = _tick_size(raw)
    return round(math.ceil(raw / tick) * tick, 2)


def notify_limit(code: str, name: str, direction: str,
                 limit_price: float, price: float):
    """
    漲停（多方）/ 跌停（空方）觸及時發出出場提醒。
    每支股票只通知一次，避免重複。
    """
    if not _NOTIFY_ENABLED:
        return
    if code in _notified_limits:
        return
    _notified_limits.add(code)
    kind  = "漲停" if direction == "多" else "跌停"
    emoji = "🚀" if direction == "多" else "🔻"
    title = f"{emoji} {code} {name}  {kind} → 考慮出場"
    body  = f"現價 {price:.2f}  {kind}價 {limit_price:.2f}"
    _send_notification(title, body)


ALERT_COOLDOWN_SEC = 300   # 同方向冷卻時間（秒）；方向反轉不受限制

def check_and_alert(code: str, name: str, sigs: dict,
                    price: float, min_hit: int) -> tuple[bool, str]:
    """
    狀態機：去除重複警報。

    觸發規則：
      1. 首次達到門檻 → 立即警報
      2. 方向反轉（多 ↔ 空）→ 立即警報
      3. 訊號集合有變化，且距上次警報已超過冷卻時間 → 警報
      4. 未達門檻 → 不觸發，也不重置（避免 tick 瞬間抖動清除狀態）

    _alert_state[code] = {"active": frozenset, "direction": str, "ts": float}
    """
    active = frozenset(k for k, v in sigs.items() if v)
    long_h    = sum(1 for k in active if not k.endswith("S"))
    short_h   = sum(1 for k in active if k.endswith("S"))
    hit       = max(long_h, short_h)
    direction = "多" if long_h >= short_h else "空"

    if hit < min_hit:
        # 低於門檻：靜默，不重置（防抖）
        return False, ""

    now_t = time.monotonic()
    last  = _alert_state.get(code)          # None = 從未警報

    if last is None:
        fire = True                          # 首次達門檻
    elif last["direction"] != direction:
        fire = True                          # 方向反轉
    elif last["active"] != active and (now_t - last["ts"]) >= ALERT_COOLDOWN_SEC:
        fire = True                          # 訊號變化 + 冷卻已過
    else:
        fire = False                         # 冷卻期內，同方向同（或類似）訊號 → 忽略

    if fire:
        _alert_state[code] = {"active": active, "direction": direction, "ts": now_t}
        sig_str = " ".join(sorted(active))
        ts      = datetime.datetime.now().strftime("%H:%M:%S")
        msg     = f"{ts} {code} {name} {direction} [{sig_str}] 現價={price:.2f}"
        notify_signal(code, name, direction, price, active)
        return True, msg

    return False, ""

# ──────────────────────────────────────────────
# 技術指標（離線預算，用昨日收盤資料）
# ──────────────────────────────────────────────
def _calc_rsi(series: pd.Series, period: int = RSI_PERIOD) -> float:
    if len(series) < period + 1:
        return 50.0
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, float("nan"))
    rsi   = 100 - 100 / (1 + rs)
    val   = rsi.iloc[-1]
    return float(val) if not pd.isna(val) else 50.0


def build_base(hist_df: pd.DataFrame) -> dict | None:
    """
    用昨日收盤資料預算所有技術指標基準，啟動時只算一次。
    回傳 dict，盤中策略判斷直接取用，不重算。
    """
    if hist_df is None or len(hist_df) < 20:
        return None

    df = hist_df.copy()
    df["Vol_K"] = df["Volume"] / 1000

    avg5 = float(df["Vol_K"].iloc[-5:].mean())
    if avg5 < MIN_AVG_VOL:
        return None

    closes  = df["Close"]
    ma5     = float(closes.iloc[-5:].mean())
    ma10    = float(closes.iloc[-10:].mean()) if len(closes) >= 10 else ma5
    ma20    = float(closes.iloc[-20:].mean())
    rsi_now = _calc_rsi(closes)
    rsi_prv = _calc_rsi(closes.iloc[:-1])

    high5 = float(df["High"].iloc[-5:].max())
    low5  = float(df["Low"].iloc[-5:].min())

    three_up = all(
        float(df["Close"].iloc[-(j+1)]) > float(df["Open"].iloc[-(j+1)])
        for j in range(1, 4)
    ) if len(df) >= 4 else False
    three_dn = all(
        float(df["Close"].iloc[-(j+1)]) < float(df["Open"].iloc[-(j+1)])
        for j in range(1, 4)
    ) if len(df) >= 4 else False

    # 昨日收盤量（爆量基準）
    vol_yesterday = float(df["Vol_K"].iloc[-1])

    # ── 新策略：均量擴張/萎縮（F/FS） ─────────────
    avg20_vol   = float(df["Vol_K"].iloc[-21:-1].mean()) if len(df) >= 22 else avg5
    vol_expand  = avg5 > avg20_vol * 1.2 if avg20_vol > 0 else False
    vol_shrink  = avg5 < avg20_vol * 0.8 if avg20_vol > 0 else False

    # ── 新策略：縮量後爆量（G/GS） ────────────────
    tide_shrink = all(
        float(df["Vol_K"].iloc[-(j+1)]) < float(df["Vol_K"].iloc[-(j+2)])
        for j in range(1, 4)
    ) if len(df) >= 5 else False

    # ── K棒型態（H/HS/I/IS）：昨日K棒基礎數據 ──────
    prev_open  = float(df["Open"].iloc[-1])
    prev_close = float(closes.iloc[-1])
    prev_body  = abs(prev_close - prev_open)
    prev_bull  = prev_close > prev_open   # 昨日陽線
    prev_bear  = prev_close < prev_open   # 昨日陰線

    # ── J/JS：MACD (12/26/9) ─────────────────────
    closes_s   = pd.Series(df["Close"].values, dtype=float)
    ema12      = closes_s.ewm(span=12, adjust=False).mean()
    ema26      = closes_s.ewm(span=26, adjust=False).mean()
    macd_line  = ema12 - ema26
    macd_sig   = macd_line.ewm(span=9, adjust=False).mean()
    macd_t     = float(macd_line.iloc[-1]) if pd.notna(macd_line.iloc[-1]) else 0.0
    macd_p     = float(macd_line.iloc[-2]) if len(macd_line) >= 2 and pd.notna(macd_line.iloc[-2]) else 0.0
    msig_t     = float(macd_sig.iloc[-1])  if pd.notna(macd_sig.iloc[-1])  else 0.0
    msig_p     = float(macd_sig.iloc[-2])  if len(macd_sig) >= 2 and pd.notna(macd_sig.iloc[-2])  else 0.0

    # ── K/KS：布林通道 (20, 2) ──────────────────
    bb_mid_s   = closes_s.rolling(20).mean()
    bb_std_s   = closes_s.rolling(20).std()
    bb_upper_s = bb_mid_s + 2 * bb_std_s
    bb_lower_s = bb_mid_s - 2 * bb_std_s
    bb_lower_t = float(bb_lower_s.iloc[-1]) if pd.notna(bb_lower_s.iloc[-1]) else float(closes_s.iloc[-1])
    bb_upper_t = float(bb_upper_s.iloc[-1]) if pd.notna(bb_upper_s.iloc[-1]) else float(closes_s.iloc[-1])
    bb_lower_p = float(bb_lower_s.iloc[-2]) if len(bb_lower_s) >= 2 and pd.notna(bb_lower_s.iloc[-2]) else bb_lower_t
    bb_upper_p = float(bb_upper_s.iloc[-2]) if len(bb_upper_s) >= 2 and pd.notna(bb_upper_s.iloc[-2]) else bb_upper_t

    # ── L/LS：KD 隨機指標 (9/3/3) ───────────────
    high_s  = pd.Series(df["High"].values, dtype=float)
    low_s   = pd.Series(df["Low"].values,  dtype=float)
    hi9     = high_s.rolling(9).max()
    lo9     = low_s.rolling(9).min()
    rsv     = ((closes_s - lo9) / (hi9 - lo9).replace(0, float("nan")) * 100).fillna(50)
    kk_s    = rsv.ewm(com=2, adjust=False).mean()
    kd_s    = kk_s.ewm(com=2, adjust=False).mean()
    kk_t    = float(kk_s.iloc[-1]) if pd.notna(kk_s.iloc[-1]) else 50.0
    kd_t    = float(kd_s.iloc[-1]) if pd.notna(kd_s.iloc[-1]) else 50.0
    kk_p    = float(kk_s.iloc[-2]) if len(kk_s) >= 2 and pd.notna(kk_s.iloc[-2]) else 50.0
    kd_p    = float(kd_s.iloc[-2]) if len(kd_s) >= 2 and pd.notna(kd_s.iloc[-2]) else 50.0

    # ── M/MS：Williams %R (14) ───────────────────
    hi14    = high_s.rolling(14).max()
    lo14    = low_s.rolling(14).min()
    wr_s    = ((hi14 - closes_s) / (hi14 - lo14).replace(0, float("nan")) * -100).fillna(-50)
    wr_t    = float(wr_s.iloc[-1]) if pd.notna(wr_s.iloc[-1]) else -50.0
    wr_p    = float(wr_s.iloc[-2]) if len(wr_s) >= 2 and pd.notna(wr_s.iloc[-2]) else -50.0

    # ── O/OS：晨星/黃昏之星 前兩日資料 ─────────
    #   day1 = hist[-2]（兩日前）, day2 = hist[-1]（昨日）, day3 = 今日盤中
    if len(df) >= 3:
        star_d1_o    = float(df["Open"].iloc[-2])
        star_d1_c    = float(df["Close"].iloc[-2])
    else:
        star_d1_o = star_d1_c = prev_close
    star_d1_body = abs(star_d1_c - star_d1_o)
    star_d1_mid  = (star_d1_c + star_d1_o) / 2
    star_d1_bear = star_d1_c < star_d1_o
    star_d1_bull = star_d1_c > star_d1_o
    star_d2_body = prev_body   # 昨日實體（day2）

    # ── P/PS：紅三兵/黑三兵 前兩日資料 ─────────
    #   d1 = hist[-2]（兩日前）, d2 = hist[-1]（昨日）, d3 = 今日盤中
    if len(df) >= 3:
        sol_d1_o = float(df["Open"].iloc[-2])
        sol_d1_c = float(df["Close"].iloc[-2])
    else:
        sol_d1_o = sol_d1_c = prev_close
    sol_d2_o = prev_open
    sol_d2_c = prev_close

    # ── Q/QS：Inside Bar 前兩日資料 ─────────────
    #   prev2 = hist[-2]（兩日前）, prev = hist[-1]（昨日）
    if len(df) >= 2:
        ib_prev2_h  = float(df["High"].iloc[-2])
        ib_prev2_l  = float(df["Low"].iloc[-2])
        ib_prev_h   = float(df["High"].iloc[-1])
        ib_prev_l   = float(df["Low"].iloc[-1])
        ib_is_inside = (ib_prev_h < ib_prev2_h) and (ib_prev_l > ib_prev2_l)
    else:
        ib_prev2_h = ib_prev2_l = 0.0
        ib_is_inside = False

    return {
        "avg5":          avg5,
        "avg20_vol":     avg20_vol,
        "vol_yesterday": vol_yesterday,
        "vol_expand":    vol_expand,
        "vol_shrink":    vol_shrink,
        "tide_shrink":   tide_shrink,
        "ma5":           ma5,
        "ma10":          ma10,
        "ma20":          ma20,
        "rsi_now":       rsi_now,
        "rsi_prv":       rsi_prv,
        "high5":         high5,
        "low5":          low5,
        "three_up":      three_up,
        "three_dn":      three_dn,
        "prev_close":    prev_close,
        "prev_open":     prev_open,
        "prev_body":     prev_body,
        "prev_bull":     prev_bull,
        "prev_bear":     prev_bear,
        "open_price":    0.0,   # 今日開盤價（盤中填入）
        # ── J/JS ──
        "macd_t":        macd_t,   "macd_p":  macd_p,
        "msig_t":        msig_t,   "msig_p":  msig_p,
        # ── K/KS ──
        "bb_lower_t":    bb_lower_t,  "bb_lower_p": bb_lower_p,
        "bb_upper_t":    bb_upper_t,  "bb_upper_p": bb_upper_p,
        # ── L/LS ──
        "kk_t":          kk_t,  "kd_t": kd_t,
        "kk_p":          kk_p,  "kd_p": kd_p,
        # ── M/MS ──
        "wr_t":          wr_t,  "wr_p": wr_p,
        # ── O/OS ──
        "star_d1_body":  star_d1_body,  "star_d1_mid":  star_d1_mid,
        "star_d1_bear":  star_d1_bear,  "star_d1_bull": star_d1_bull,
        "star_d2_body":  star_d2_body,
        # ── P/PS ──
        "sol_d1_o":      sol_d1_o,  "sol_d1_c": sol_d1_c,
        "sol_d2_o":      sol_d2_o,  "sol_d2_c": sol_d2_c,
        # ── Q/QS ──
        "ib_prev2_h":    ib_prev2_h,  "ib_prev2_l":  ib_prev2_l,
        "ib_is_inside":  ib_is_inside,
    }


def check_signals(base: dict, price: float, open_p: float, chg_pct: float,
                  vol_mult: float,
                  high_p: float = 0, low_p: float = 0) -> dict:
    """
    盤中策略判斷。所有技術指標基準來自 build_base()，盤中不重算。
    price / open_p / chg_pct / high_p / low_p 均為即時資料。

    策略對照（與 swing_trade_v2.py 一致，共 37 個）：
      A/AS   均線突破/死亡         B/BS   開盤跳空缺口
      C/CS   RSI 超賣/超買反轉     D/DS   突破/跌破近 5 日高低
      E/ES   強勢連漲/弱勢連跌     F/FS   均量擴張/萎縮
      G/GS   縮量後上漲/爆跌       H/HS   鎚子K / 射擊之星
      I/IS   吞噬陽線 / 吞噬陰線   J/JS   MACD 黃金/死亡交叉
      K/KS   布林下軌反彈/上軌反壓 L/LS   KD 超賣/超買交叉
      M/MS   威廉%R 超賣/超買      N/NS   多頭/空頭排列回測站回/跌破
      O/OS   晨星 / 黃昏之星       P/PS   紅三兵 / 黑三兵
      Q/QS   Inside Bar 突破/跌破  R/RS   BIAS 超跌反彈/超漲回落
      B2     大跳空(≥5%)+低量(≥0.8x)
    ★ 多方策略 A/B/D/F/G 已移除爆量條件（量多反而降低多方 EV）
    """
    ALL_KEYS = ["A","AS","B","BS","C","CS","D","DS","E","ES",
                "F","FS","G","GS","H","HS","I","IS",
                "J","JS","K","KS","L","LS","M","MS",
                "N","NS","O","OS","P","PS","Q","QS","R","RS","B2"]
    empty = {k: False for k in ALL_KEYS}
    if price == 0:
        return empty

    # ── 基礎數值 ──────────────────────────────────
    avg5          = base["avg5"]
    vol_yesterday = base["vol_yesterday"]
    ma5           = base["ma5"]
    ma10          = base["ma10"]
    ma20          = base["ma20"]
    rsi_now       = base["rsi_now"]
    rsi_prv       = base["rsi_prv"]
    high5         = base["high5"]
    low5          = base["low5"]
    three_up      = base["three_up"]
    three_dn      = base["three_dn"]
    prev_close    = base["prev_close"]
    vol_expand    = base.get("vol_expand",  False)
    vol_shrink    = base.get("vol_shrink",  False)
    tide_shrink   = base.get("tide_shrink", False)
    prev_open     = base.get("prev_open",   price)
    prev_body     = base.get("prev_body",   0.0)
    prev_bull     = base.get("prev_bull",   False)
    prev_bear     = base.get("prev_bear",   False)

    # 爆量：昨日收盤量 / 5日均量（固定基準，不受盤中累積量影響）
    vol_ratio = vol_yesterday / avg5 if avg5 > 0 else 0
    gap_pct   = (open_p - prev_close) / prev_close * 100 if prev_close else 0

    # ── K棒型態（H/I/O/P）：需要今日盤中即時 O/H/L/C ──
    body = upper = lower = 0.0
    is_bull = is_bear = False
    hammer = shoot_star = engulf_bull = engulf_bear = False
    if open_p > 0:
        body    = abs(price - open_p)
        is_bull = price > open_p
        is_bear = price < open_p
    if high_p > 0 and low_p > 0 and open_p > 0:
        upper   = high_p - max(price, open_p)
        lower   = min(price, open_p) - low_p
        # 鎚子K：下影線 ≥ 2×實體，上影線 ≤ 實體，前日收跌
        hammer      = (body > 0 and lower >= 2*body and upper <= body
                       and chg_pct < 0 and prev_bear)
        # 射擊之星：上影線 ≥ 2×實體，下影線 ≤ 實體，前日收漲
        shoot_star  = (body > 0 and upper >= 2*body and lower <= body
                       and chg_pct > 0 and prev_bull)
        # 吞噬陽線：今日陽線包覆昨日陰線
        engulf_bull = (is_bull and prev_bear
                       and open_p <= prev_close and price >= prev_open
                       and body > prev_body * 0.8)
        # 吞噬陰線：今日陰線包覆昨日陽線
        engulf_bear = (is_bear and prev_bull
                       and open_p >= prev_close and price <= prev_open
                       and body > prev_body * 0.8)

    # ── 晨星 / 黃昏之星（O/OS）────────────────────
    d1_body    = base.get("star_d1_body", 0.0)
    d1_mid     = base.get("star_d1_mid",  0.0)
    d1_bear_s  = base.get("star_d1_bear", False)
    d1_bull_s  = base.get("star_d1_bull", False)
    d2_body    = base.get("star_d2_body", 0.0)
    morning_star = (
        d1_bear_s and d1_body > 0 and
        d2_body < d1_body * 0.4 and
        is_bull and price > d1_mid and body >= d1_body * 0.5
    )
    evening_star = (
        d1_bull_s and d1_body > 0 and
        d2_body < d1_body * 0.4 and
        is_bear and price < d1_mid and body >= d1_body * 0.5
    )

    # ── 紅三兵 / 黑三兵（P/PS）──────────────────
    sol_d1_o = base.get("sol_d1_o", 0.0)
    sol_d1_c = base.get("sol_d1_c", 0.0)
    sol_d2_o = base.get("sol_d2_o", 0.0)
    sol_d2_c = base.get("sol_d2_c", 0.0)
    three_soldiers = (
        sol_d1_c > sol_d1_o and sol_d2_c > sol_d2_o and is_bull and
        sol_d2_c > sol_d1_c and price > sol_d2_c and
        sol_d2_o >= sol_d1_o and open_p >= sol_d2_o
    )
    three_crows = (
        sol_d1_c < sol_d1_o and sol_d2_c < sol_d2_o and is_bear and
        sol_d2_c < sol_d1_c and price < sol_d2_c and
        sol_d2_o <= sol_d1_o and open_p <= sol_d2_o
    )

    # ── Inside Bar 突破/跌破（Q/QS）─────────────
    ib_prev2_h   = base.get("ib_prev2_h",   0.0)
    ib_prev2_l   = base.get("ib_prev2_l",   0.0)
    ib_is_inside = base.get("ib_is_inside", False)
    inside_breakout  = ib_is_inside and (price > ib_prev2_h)
    inside_breakdown = ib_is_inside and (price < ib_prev2_l) and (vol_ratio >= vol_mult)

    # ── BIAS 乖離率（R/RS）──────────────────────
    bias = (price - ma20) / ma20 * 100 if ma20 > 0 else 0.0

    # ── MA 排列回測（N/NS）──────────────────────
    ma_bull_align = ma5 > ma10 > ma20
    ma_bear_align = ma5 < ma10 < ma20

    return {
        # ── A–E（多方移除爆量條件，空方保留）───────
        "A":  bool(ma5 > ma20 and chg_pct > 0),                       # ★ 移除 vol_ratio
        "AS": bool(ma5 < ma20 and vol_ratio >= vol_mult and chg_pct < 0),
        "B":  bool(gap_pct >= 2.0 and chg_pct > 0),                   # ★ 移除 1.3x
        "BS": bool(gap_pct <= -2.0 and vol_ratio >= 1.3 and chg_pct < 0),
        "C":  bool(rsi_prv < 35 and rsi_now > rsi_prv and price > ma5),
        "CS": bool(rsi_prv > 65 and rsi_now < rsi_prv and price < ma5),
        "D":  bool(price > high5),                                     # ★ 移除 vol_ratio
        "DS": bool(price < low5 and vol_ratio >= vol_mult),
        "E":  bool(three_up and ma5 > ma10 > ma20 and chg_pct > 0),
        "ES": bool(three_dn and ma5 < ma10 < ma20 and chg_pct < 0),
        # ── F–I ─────────────────────────────────────
        "F":  bool(vol_expand  and chg_pct > 0),                       # ★ 移除 vol_ratio
        "FS": bool(vol_shrink  and vol_ratio >= vol_mult and chg_pct < 0),
        "G":  bool(tide_shrink and chg_pct > 0),                       # ★ 移除 vol_ratio
        "GS": bool(tide_shrink and vol_ratio >= vol_mult and chg_pct < 0),
        "H":  bool(hammer),
        "HS": bool(shoot_star),
        "I":  bool(engulf_bull),
        "IS": bool(engulf_bear),
        # ── J/JS：MACD 黃金/死亡交叉 ────────────────
        "J":  bool(base["macd_p"] < base["msig_p"] and base["macd_t"] > base["msig_t"] and chg_pct > 0),
        "JS": bool(base["macd_p"] > base["msig_p"] and base["macd_t"] < base["msig_t"] and chg_pct < 0),
        # ── K/KS：布林下軌反彈 / 上軌反壓 ──────────
        "K":  bool(prev_close <= base["bb_lower_p"] and price > base["bb_lower_t"]),
        "KS": bool(prev_close >= base["bb_upper_p"] and price < base["bb_upper_t"]),
        # ── L/LS：KD 超賣黃金交叉 / 超買死亡交叉 ────
        "L":  bool(base["kk_t"] < 30 and base["kk_p"] < base["kd_p"] and base["kk_t"] > base["kd_t"]),
        "LS": bool(base["kk_t"] > 70 and base["kk_p"] > base["kd_p"] and base["kk_t"] < base["kd_t"]),
        # ── M/MS：威廉%R 超賣反彈 / 超買回落 ────────
        "M":  bool(base["wr_p"] < -80 and base["wr_t"] > base["wr_p"] and chg_pct > 0),
        "MS": bool(base["wr_p"] > -20 and base["wr_t"] < base["wr_p"] and chg_pct < 0),
        # ── N/NS：多頭/空頭排列 MA5 回測 ────────────
        "N":  bool(ma_bull_align and prev_close < ma5 and price >= ma5),
        "NS": bool(ma_bear_align and prev_close > ma5 and price <= ma5),
        # ── O/OS：晨星 / 黃昏之星 ───────────────────
        "O":  bool(morning_star),
        "OS": bool(evening_star),
        # ── P/PS：紅三兵 / 黑三兵 ───────────────────
        "P":  bool(three_soldiers),
        "PS": bool(three_crows),
        # ── Q/QS：Inside Bar 突破 / 跌破 ────────────
        "Q":  bool(inside_breakout),
        "QS": bool(inside_breakdown),
        # ── R/RS：BIAS 超跌反彈 / 超漲回落 ──────────
        "R":  bool(bias < -8  and chg_pct > 0),
        "RS": bool(bias > +8  and chg_pct < 0),
        # ── B2：大跳空(≥5%) + 量比≥0.8x ─────────────
        "B2": bool(gap_pct >= 5.0 and vol_ratio >= 0.8 and chg_pct > 0),
    }


# ──────────────────────────────────────────────
# WebSocket 廣播伺服器
# ──────────────────────────────────────────────
class WsBroadcaster:
    """
    在獨立 asyncio 執行緒中跑 WebSocket server。
    主執行緒呼叫 broadcast(dict) 即可推送 JSON 給所有已連線的客戶端。
    pip install websockets
    """
    def __init__(self, port: int = 8765):
        self._port    = port
        self._clients: set = set()
        self._loop:   asyncio.AbstractEventLoop | None = None
        self._lock    = threading.Lock()

    def start(self) -> bool:
        if not HAS_WS:
            print("  ⚠️  websockets 未安裝，WebSocket server 停用（pip install websockets）")
            return False
        ready = threading.Event()
        t = threading.Thread(target=self._run, args=(ready,), daemon=True)
        t.start()
        if not ready.wait(timeout=5):
            print("  ⚠️  WebSocket server 啟動逾時")
            return False
        print(f"  🌐 WebSocket server 啟動：ws://0.0.0.0:{self._port}")
        return True

    def _run(self, ready: threading.Event):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve(ready))

    async def _serve(self, ready: threading.Event):
        async with websockets.serve(self._handler, "0.0.0.0", self._port):
            ready.set()
            await asyncio.Future()   # run forever

    async def _handler(self, ws):
        with self._lock:
            self._clients.add(ws)
        try:
            await ws.wait_closed()
        finally:
            with self._lock:
                self._clients.discard(ws)

    def broadcast(self, data: dict):
        if not HAS_WS or self._loop is None:
            return
        msg = json.dumps(data, ensure_ascii=False)
        asyncio.run_coroutine_threadsafe(self._send_all(msg), self._loop)

    async def _send_all(self, msg: str):
        with self._lock:
            clients = list(self._clients)
        if clients:
            await asyncio.gather(*[c.send(msg) for c in clients],
                                 return_exceptions=True)


def _build_ws_payload(signals: dict, pm, alerts: list,
                      scan_n: int, min_hit: int) -> dict:
    """將目前掃描結果打包成 JSON-serializable dict 供 WebSocket 推送。"""
    ws_signals = []
    for code, info in signals.items():
        sigs = info["sigs"]
        q    = info["quote"]
        long_hit  = sum(1 for k, v in sigs.items() if v and not k.endswith("S"))
        short_hit = sum(1 for k, v in sigs.items() if v and k.endswith("S"))
        ws_signals.append({
            "code":           code,
            "name":           q.get("name", code),
            "price":          round(q.get("price", 0), 2),
            "chg_pct":        round(q.get("chg_pct", 0), 2),
            "hit_long":       long_hit,
            "hit_short":      short_hit,
            "hit":            max(long_hit, short_hit),
            "direction":      "多" if long_hit >= short_hit else "空",
            "active_signals": sorted(k for k, v in sigs.items() if v),
            "update_time":    q.get("time", ""),
        })
    ws_signals.sort(key=lambda x: x["hit"], reverse=True)

    positions = []
    for pos in pm.get_positions():
        positions.append({
            "code":       pos["code"],
            "name":       pos["name"],
            "direction":  pos["direction"],
            "entry":      pos["entry"],
            "stop":       pos["stop"],
            "curr_price": pos.get("curr_price", pos["entry"]),
            "pnl_pct":    pos.get("pnl_pct", 0.0),
            "status":     pos["status"],
            "entered_at": pos["entered_at"],
        })

    return {
        "type":      "update",
        "timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
        "scan_n":    scan_n,
        "min_hit":   min_hit,
        "signals":   ws_signals,
        "positions": positions,
        "alerts":    alerts[-30:],
        "summary":   pm.summary(),
    }


# ──────────────────────────────────────────────
# 當日部位管理（記憶體）
# ──────────────────────────────────────────────
class PositionManager:
    """
    記錄當日進出場狀態。
    所有資料存在記憶體，程式關閉自動清空（符合當沖邏輯）。
    """
    def __init__(self, stop_loss_pct: float = DEFAULT_STOP):
        self.stop_loss_pct = stop_loss_pct
        self._positions: dict = {}   # code -> position dict
        self._history:   list = []   # 已平倉紀錄
        self._lock = threading.Lock()

    def enter(self, code: str, name: str, direction: str,
              price: float, signals: list[str]):
        """記錄進場"""
        with self._lock:
            if code in self._positions:
                return  # 已有部位，不重複進場
            sl = price * (1 - self.stop_loss_pct / 100) if direction == "多" \
                 else price * (1 + self.stop_loss_pct / 100)
            self._positions[code] = {
                "code":      code,
                "name":      name,
                "direction": direction,
                "entry":     price,
                "stop":      round(sl, 2),
                "signals":   signals,
                "entered_at": datetime.datetime.now().strftime("%H:%M:%S"),
                "pnl_pct":   0.0,
                "status":    "持倉",
            }

    def update_price(self, code: str, price: float):
        """更新現價，計算損益，檢查停損"""
        with self._lock:
            if code not in self._positions:
                return
            pos = self._positions[code]
            entry = pos["entry"]
            if pos["direction"] == "多":
                pnl = (price - entry) / entry * 100
                stopped = price <= pos["stop"]
            else:
                pnl = (entry - price) / entry * 100
                stopped = price >= pos["stop"]
            pos["pnl_pct"]    = round(pnl, 2)
            pos["curr_price"] = price
            if stopped and pos["status"] == "持倉":
                pos["status"] = "停損"
                self._close(code, price, "停損")

    def _close(self, code: str, price: float, reason: str):
        """平倉（內部呼叫，已持鎖）"""
        pos = self._positions.pop(code, None)
        if pos:
            pos["exit"]      = price
            pos["exit_time"] = datetime.datetime.now().strftime("%H:%M:%S")
            pos["reason"]    = reason
            self._history.append(pos)

    def exit(self, code: str, price: float, reason: str = "手動平倉"):
        """手動平倉"""
        with self._lock:
            self._close(code, price, reason)

    def get_positions(self) -> list:
        with self._lock:
            return list(self._positions.values())

    def get_history(self) -> list:
        with self._lock:
            return list(self._history)

    def summary(self) -> dict:
        """今日損益彙整"""
        hist = self.get_history()
        if not hist:
            return {"trades": 0, "win": 0, "lose": 0, "total_pnl": 0.0}
        wins   = [h for h in hist if h.get("pnl_pct", 0) > 0]
        losses = [h for h in hist if h.get("pnl_pct", 0) <= 0]
        return {
            "trades":    len(hist),
            "win":       len(wins),
            "lose":      len(losses),
            "total_pnl": round(sum(h.get("pnl_pct", 0) for h in hist), 2),
        }


# ──────────────────────────────────────────────
# Shioaji 即時行情管理
# ──────────────────────────────────────────────
class LiveQuote:
    """
    封裝 Shioaji Tick 訂閱。
    有成交才推送，callback 更新記憶體，主迴圈直接讀取，無需 polling。
    """
    def __init__(self):
        self._api    = None
        self._quotes: dict = {}
        self._lock   = threading.Lock()
        self._ready  = False

    def get_api(self):
        """回傳已登入的 Shioaji api 物件（供 fetch_history 重用）"""
        return self._api

    def login(self, api_key: str, secret_key: str) -> bool:
        try:
            import shioaji as sj
        except ImportError:
            print("  ❌ 請先安裝：pip install shioaji")
            return False
        try:
            print("  🔑 連線 Shioaji...")
            self._api = sj.Shioaji()
            _null = io.StringIO()
            with contextlib.redirect_stdout(_null), \
                 contextlib.redirect_stderr(_null):
                accounts = self._api.login(
                    api_key=api_key,
                    secret_key=secret_key,
                    contracts_timeout=30000,
                )
            # 等待合約載入完成
            print("  ⏳ 等待合約載入...")
            for _ in range(60):
                try:
                    if self._api.Contracts.Stocks["2330"] is not None:
                        break
                except Exception:
                    pass
                time.sleep(1)
            time.sleep(3)   # SOLACE session 穩定
            print(f"  ✅ 登入成功，帳號：{len(accounts)} 個")
            self._ready = True
            return True
        except Exception as e:
            print(f"  ❌ 登入失敗：{e}")
            return False

    def subscribe(self, codes: list, bases: dict):
        """訂閱股票 Tick，並用 snapshots 初始化昨收價"""
        if not self._ready:
            return

        import shioaji as sj

        # Tick callback：有成交才推送，直接更新記憶體
        @self._api.on_tick_stk_v1()
        def _on_tick(exchange, tick):
            code  = tick.code
            price = float(tick.close)
            with self._lock:
                q = self._quotes.get(code, {})
                prev = q.get("prev_close", 0)
                chg  = round((price - prev) / prev * 100, 2) if prev else 0
                self._quotes[code] = {
                    **q,
                    "price":       price,
                    "high":        float(tick.high),
                    "low":         float(tick.low),
                    "chg_pct":     chg,
                    "total_vol":   float(tick.total_volume),
                    "time":        tick.datetime.strftime("%H:%M:%S"),
                }

        # Snapshot 取昨收、開盤
        print(f"  📸 取得 {len(codes)} 檔快照...")
        batch = 200
        for i in range(0, len(codes), batch):
            sub = codes[i:i+batch]
            contracts = [self._api.Contracts.Stocks[c]
                         for c in sub if self._api.Contracts.Stocks.get(c)]
            if not contracts:
                continue
            try:
                snaps = self._api.snapshots(contracts)
                for s in snaps:
                    prev_cl = float(s.close) - float(s.change_price)
                    with self._lock:
                        contract = self._api.Contracts.Stocks.get(s.code)
                        name = getattr(contract, "name", s.code) if contract else s.code
                        self._quotes[s.code] = {
                            "name":       name,
                            "price":      float(s.close),
                            "open":       float(s.open),
                            "high":       float(s.high),
                            "low":        float(s.low),
                            "prev_close": prev_cl,
                            "chg_pct":    round(float(s.change_rate), 2),
                            "total_vol":  float(s.total_volume),
                            "yesterday_vol": float(s.yesterday_volume or 0),
                            "time":       "",
                        }
                    # 把昨日量回填到 base（供爆量計算用）
                    if s.code in bases:
                        bases[s.code]["vol_yesterday"] = float(s.yesterday_volume or 0)
                        bases[s.code]["open_price"]    = float(s.open)
                        bases[s.code]["prev_close"]    = prev_cl
            except Exception as e:
                print(f"  ⚠️  Snapshot 批次失敗：{e}")

        # 訂閱 Tick（壓掉 Shioaji 的 Response Code 200 確認訊息）
        subscribed = 0
        _null = io.StringIO()
        for code in codes:
            c = self._api.Contracts.Stocks.get(code)
            if c is None:
                continue
            try:
                with contextlib.redirect_stdout(_null), \
                     contextlib.redirect_stderr(_null):
                    self._api.quote.subscribe(
                        c,
                        quote_type=sj.constant.QuoteType.Tick,
                        version=sj.constant.QuoteVersion.v1,
                    )
                subscribed += 1
            except Exception:
                pass
        print(f"  ✅ 訂閱完成：{subscribed} 檔（Tick 推送，無延遲）")

    def get(self, code: str) -> dict:
        with self._lock:
            return dict(self._quotes.get(code, {}))

    def get_all(self) -> dict:
        with self._lock:
            return dict(self._quotes)

    def logout(self):
        if self._api:
            try:
                self._api.logout()
                print("  👋 Shioaji 已登出")
            except Exception:
                pass


# ──────────────────────────────────────────────
# 顯示工具
# ──────────────────────────────────────────────
def _dw(s: str) -> int:
    """計算終端機顯示寬度（處理中文、emoji）"""
    w = 0
    for c in str(s):
        w += 2 if unicodedata.east_asian_width(c) in ('W', 'F', 'A') else 1
    return w

def _pad(s: str, width: int, align: str = "left") -> str:
    s = str(s)
    pad = max(0, width - _dw(s))
    return (" " * pad + s) if align == "right" else (s + " " * pad)

RIGHT_COLS = {"現價","漲跌幅(%)","成交量(張)","命中數","進場價","停損價","損益(%)"}

def print_table(df: pd.DataFrame, cols: list):
    widths = {}
    for col in cols:
        widths[col] = max(_dw(str(col)), max((_dw(str(v)) for v in df[col]), default=0)) + 1
    sep = "  " + "─" * (sum(widths.values()) + len(cols))
    header = "  " + " ".join(_pad(c, widths[c], "right" if c in RIGHT_COLS else "left") for c in cols)
    print(header)
    print(sep)
    for _, row in df.iterrows():
        line = "  " + " ".join(
            _pad(str(row[c]), widths[c], "right" if c in RIGHT_COLS else "left")
            for c in cols
        )
        print(line)


def _pnl_color(pnl: float) -> str:
    if pnl > 0:  return "\033[32m"
    if pnl < 0:  return "\033[31m"
    return ""
RESET = "\033[0m"


def render(positions: list, signals: dict, scan_n: int,
           alerts: list, summary: dict, min_hit: int):
    """主畫面渲染"""
    os.system("cls" if os.name == "nt" else "clear")
    now = datetime.datetime.now().strftime("%H:%M:%S")

    print("═"*100)
    print(f"  🔴 台股日當沖即時系統  {now}  第 {scan_n} 次更新  命中門檻：≥{min_hit}")
    print("═"*100)

    # ── 當日部位 ─────────────────────────────
    if positions:
        print(f"\n  📊 當日部位（{len(positions)} 筆）")
        pos_df = pd.DataFrame(positions)
        pos_df["損益(%)"] = pos_df["pnl_pct"].apply(lambda x: f"{x:+.2f}%")
        cols = ["code","name","direction","entry","stop","curr_price","損益(%)","status","entered_at"]
        col_rename = {"code":"代號","name":"名稱","direction":"方向",
                      "entry":"進場價","stop":"停損價",
                      "curr_price":"現價","entered_at":"進場時間"}
        pos_df.rename(columns=col_rename, inplace=True)
        display = ["代號","名稱","方向","進場價","停損價","現價","損益(%)","status","進場時間"]
        display = [c for c in display if c in pos_df.columns]
        print_table(pos_df, display)

    # ── 訊號清單 ─────────────────────────────
    col_map = {
        "A":"A多",  "AS":"AS空", "B":"B多",  "BS":"BS空",
        "C":"C多",  "CS":"CS空", "D":"D多",  "DS":"DS空",
        "E":"E多",  "ES":"ES空", "F":"F多",  "FS":"FS空",
        "G":"G多",  "GS":"GS空", "H":"H多",  "HS":"HS空",
        "I":"I多",  "IS":"IS空", "J":"J多",  "JS":"JS空",
        "K":"K多",  "KS":"KS空", "L":"L多",  "LS":"LS空",
        "M":"M多",  "MS":"MS空", "N":"N多",  "NS":"NS空",
        "O":"O多",  "OS":"OS空", "P":"P多",  "PS":"PS空",
        "Q":"Q多",  "QS":"QS空", "R":"R多",  "RS":"RS空",
        "B2":"B2多",
    }
    rows = []
    for code, info in signals.items():
        sigs = info["sigs"]
        q    = info["quote"]
        long_hit  = sum(1 for k,v in sigs.items() if v and not k.endswith("S"))
        short_hit = sum(1 for k,v in sigs.items() if v and k.endswith("S"))
        if max(long_hit, short_hit) < min_hit:
            continue
        # 建立欄位 dict，B多若同時觸發 B2 則標 ★（缺口≥5%，EV 更強）
        sig_cells = {col_map[k]: "✅" if v else "❌" for k, v in sigs.items()}
        if sigs.get("B") and sigs.get("B2"):
            sig_cells["B多"] = "✅★"   # B2 強化訊號（跳空≥5%，EV 高 0.4~0.8%）
        rows.append({
            "代號":      code,
            "名稱":      q.get("name", code),
            "現價":      round(q.get("price", 0), 2),
            "漲跌幅(%)": f"{q.get('chg_pct', 0):+.2f}%",
            "成交量(張)": int(q.get("total_vol", 0)),
            **sig_cells,
            "命中數":    max(long_hit, short_hit),
            "更新":      q.get("time", ""),
        })

    if rows:
        sig_df = pd.DataFrame(rows).sort_values(["命中數","漲跌幅(%)"], ascending=[False,False])
        print(f"\n  📡 訊號清單（{len(sig_df)} 檔通過門檻）")
        display_cols = ["代號","名稱","現價","漲跌幅(%)","成交量(張)",
                        "A多","AS空","B多","BS空","C多","CS空","D多","DS空","E多","ES空",
                        "F多","FS空","G多","GS空","H多","HS空","I多","IS空",
                        "J多","JS空","K多","KS空","L多","LS空","M多","MS空",
                        "N多","NS空","O多","OS空","P多","PS空","Q多","QS空",
                        "R多","RS空","B2多",
                        "命中數","更新"]
        print_table(sig_df, display_cols)
    else:
        print(f"\n  ─ 目前無股票達到命中門檻 ≥{min_hit}")

    # ── 今日損益彙整 ─────────────────────────
    if summary["trades"] > 0:
        wr = summary["win"] / summary["trades"] * 100
        c  = _pnl_color(summary["total_pnl"])
        print(f"\n  💰 今日已平倉：{summary['trades']} 筆  "
              f"勝率 {wr:.0f}%  "
              f"總損益 {c}{summary['total_pnl']:+.2f}%{RESET}")

    # ── 最近警報 ─────────────────────────────
    if alerts:
        print(f"\n  🔔 最近警報：")
        for a in alerts[-5:]:
            print(f"     {a}")

    print("\n" + "═"*100)
    print("  策略：A多=均線突破  AS空=均線死亡+爆量  B多=跳空↑  BS空=跳空↓+爆量  "
          "C多=RSI超賣  CS空=RSI超買  D多=突破前高  DS空=跌破前低  E多=強勢連漲  ES空=弱勢連跌")
    print("        F多=均量擴張  FS空=均量萎縮+爆量  G多=縮後上漲  GS空=縮後爆跌  "
          "H多=鎚子K   HS空=射擊之星  I多=吞噬陽線  IS空=吞噬陰線")
    print("        J多=MACD黃叉  JS空=MACD死叉  K多=布林下軌  KS空=布林上軌  "
          "L多=KD<30黃叉  LS空=KD>70死叉  M多=威廉超賣  MS空=威廉超買")
    print("        N多=MA回測站回  NS空=MA回測跌破  O多=晨星  OS空=黃昏星  "
          "P多=紅三兵  PS空=黑三兵  Q多=IB突破  QS空=IB跌破  R多=BIAS超跌  RS空=BIAS超漲  B2多=大跳空≥5%")
    print("  ★ 標記：B多顯示 ✅★ 表示同時觸發 B2（缺口≥5%），此時配合趨勢策略 EV 可提升 +0.4~+0.8%")
    print("  （Ctrl+C 停止）")


# ──────────────────────────────────────────────
# 歷史資料下載（永豐金 kbars 主源 + TWSE 官方備援）
# ──────────────────────────────────────────────
def _fetch_twse_stock(code: str, start: datetime.date, end: datetime.date) -> pd.DataFrame | None:
    """
    TWSE/TPEX 官方 API 抓單支股票歷史日 K。
    上市用 twse.com.tw，上櫃用 tpex.org.tw。
    回傳 DataFrame(Date, Open, High, Low, Close, Volume) 或 None。
    """
    import requests, re

    months = []
    cur = start.replace(day=1)
    while cur <= end:
        months.append(cur.strftime("%Y%m"))
        cur = (cur + datetime.timedelta(days=32)).replace(day=1)

    rows_all = []
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})
    sess.verify = False

    for ym in months:
        # 先試上市（TWSE）
        fetched = False
        for attempt in range(2):
            try:
                url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                       f"?response=json&date={ym}01&stockNo={code}")
                r = sess.get(url, timeout=10)
                if r.status_code == 200:
                    j = r.json()
                    if j.get("stat") == "OK" and j.get("data"):
                        for row in j["data"]:
                            try:
                                # 民國年轉西元
                                parts = row[0].split("/")
                                year  = int(parts[0]) + 1911
                                date  = datetime.date(year, int(parts[1]), int(parts[2]))
                                if not (start <= date <= end):
                                    continue
                                def _clean(s):
                                    return float(re.sub(r"[,X]", "", s) or "0")
                                rows_all.append({
                                    "Date":   date,
                                    "Open":   _clean(row[3]),
                                    "High":   _clean(row[4]),
                                    "Low":    _clean(row[5]),
                                    "Close":  _clean(row[6]),
                                    "Volume": _clean(row[1]) * 1000,
                                })
                            except Exception:
                                pass
                        fetched = True
                        break
            except Exception:
                pass
            time.sleep(0.3)

        # 若 TWSE 無資料，試上櫃（TPEX）
        if not fetched:
            try:
                roc_ym = f"{int(ym[:4]) - 1911}/{ym[4:]}"
                url    = (f"https://www.tpex.org.tw/web/stock/aftertrading/"
                          f"daily_trading_info/st43_result.php"
                          f"?l=zh-tw&d={roc_ym}&stkno={code}&_=1")
                r = sess.get(url, timeout=10)
                if r.status_code == 200:
                    j = r.json()
                    for row in j.get("aaData", []):
                        try:
                            parts = row[0].split("/")
                            year  = int(parts[0]) + 1911
                            date  = datetime.date(year, int(parts[1]), int(parts[2]))
                            if not (start <= date <= end):
                                continue
                            def _clean2(s):
                                return float(re.sub(r"[,X]", "", s) or "0")
                            rows_all.append({
                                "Date":   date,
                                "Open":   _clean2(row[4]),
                                "High":   _clean2(row[5]),
                                "Low":    _clean2(row[6]),
                                "Close":  _clean2(row[7]),
                                "Volume": _clean2(row[1]) * 1000,
                            })
                        except Exception:
                            pass
            except Exception:
                pass
        time.sleep(0.2)

    if not rows_all:
        return None
    df = pd.DataFrame(rows_all).sort_values("Date").drop_duplicates("Date")
    df = df.set_index("Date")
    df.index = pd.to_datetime(df.index)
    return df if len(df) >= 20 else None


def fetch_history(codes: list, api=None, days: int = 150) -> dict:
    """
    下載近 {days} 日歷史資料，用於預算技術指標基準。
    資料源優先順序：
      1. 永豐金 kbars（api != None 時）— 與 swing_trade_v2.py 相同邏輯
      2. TWSE / TPEX 官方 API 備援（免帳號）
    """
    today      = datetime.date.today()
    hist_end   = today - datetime.timedelta(days=1)   # kbars 不含當日盤中
    hist_start = today - datetime.timedelta(days=days + 40)
    start_str  = str(hist_start)
    end_str    = str(hist_end)

    print(f"  📥 下載歷史資料 {hist_start} ~ {hist_end}（共 {len(codes)} 檔）...")

    results        = {}
    fallback_codes = []

    # ── 階段 1：永豐金 kbars ────────────────────────
    if api is not None:
        print("  📡 永豐金 kbars 下載中...")

        def _get_contract(code: str):
            for mkt in ("TSE", "OTC"):
                try:
                    c = getattr(api.Contracts.Stocks, mkt)[code]
                    if c is not None:
                        return c
                except Exception:
                    pass
            try:
                return api.Contracts.Stocks[code]
            except Exception:
                return None

        # smoke test
        _smoke_ok = False
        _sc = _get_contract("2330")
        if _sc:
            try:
                _kb = api.kbars(contract=_sc,
                                start=str(hist_end - datetime.timedelta(days=5)),
                                end=str(hist_end))
                if _kb and len(pd.DataFrame({**_kb})) > 0:
                    _smoke_ok = True
            except Exception as e:
                print(f"  ⚠️  kbars smoke test 失敗（{e}），改走 TWSE 備援")

        if not _smoke_ok:
            fallback_codes = list(codes)
        else:
            total = len(codes)
            for idx, code in enumerate(codes, 1):
                contract = _get_contract(code)
                if contract is None:
                    fallback_codes.append(code)
                    continue

                kb, last_err = None, None
                for _attempt in range(3):
                    for freq_kwarg in [{"frequency": "D"}, {}]:
                        try:
                            kb = api.kbars(contract=contract,
                                           start=start_str, end=end_str,
                                           **freq_kwarg)
                            break
                        except TypeError as te:
                            if "frequency" in str(te):
                                continue
                            last_err = te; break
                        except Exception as e:
                            last_err = e; break
                    if kb is not None:
                        break
                    if last_err and "Not ready" in str(last_err):
                        time.sleep(1)
                    else:
                        break

                if kb is None:
                    fallback_codes.append(code)
                    continue

                try:
                    df_min = pd.DataFrame({**kb})
                    if len(df_min) == 0:
                        fallback_codes.append(code)
                        continue
                    df_min["ts"]   = pd.to_datetime(df_min["ts"])
                    df_min["date"] = df_min["ts"].dt.date
                    df_day = df_min.groupby("date").agg(
                        Open   = ("Open",   "first"),
                        High   = ("High",   "max"),
                        Low    = ("Low",    "min"),
                        Close  = ("Close",  "last"),
                        Volume = ("Volume", "sum"),
                    ).reset_index()
                    df_day.index = pd.to_datetime(df_day["date"])
                    df_day.index.name = None
                    df_day = df_day.drop(columns=["date"])
                    df_day["Volume"] = df_day["Volume"] * 1000   # 張 → 股
                    df_day = df_day[(df_day.index.date >= hist_start) &
                                    (df_day.index.date <= hist_end)]
                    if len(df_day) >= 20:
                        results[code] = df_day
                    else:
                        fallback_codes.append(code)
                except Exception:
                    fallback_codes.append(code)

                if idx % 10 == 0 or idx == total:
                    print(f"     kbars: {idx}/{total} 檔"
                          f"（成功 {len(results)} / 備援 {len(fallback_codes)}）", end="\r")
            print()
            print(f"  ✅ kbars 完成：{len(results)} 檔 | 備援 {len(fallback_codes)} 檔")
    else:
        # 沒有 api 物件（非盤時測試），全走 TWSE
        fallback_codes = list(codes)

    # ── 階段 2：TWSE / TPEX 備援 ──────────────────
    if fallback_codes:
        print(f"  🔄 TWSE/TPEX 補抓 {len(fallback_codes)} 檔...")
        import warnings
        warnings.filterwarnings("ignore")
        for i, code in enumerate(fallback_codes, 1):
            df = _fetch_twse_stock(code, hist_start, hist_end)
            if df is not None:
                results[code] = df
            if i % 5 == 0 or i == len(fallback_codes):
                print(f"     TWSE: {i}/{len(fallback_codes)} 檔，累計成功 {len(results)} 檔...", end="\r")
        print()

    print(f"  ✅ 歷史資料：{len(results)}/{len(codes)} 檔")
    return results


# ──────────────────────────────────────────────
# 載入候選股清單
# ──────────────────────────────────────────────
def load_codes(csv_path: str = None, codes_str: str = None) -> list:
    codes = []
    if csv_path:
        try:
            df  = pd.read_csv(csv_path)
            col = next((c for c in df.columns if "代號" in c), None)
            if col:
                codes += df[col].astype(str).str.strip().tolist()
                print(f"  📋 從 CSV 載入：{len(codes)} 檔")
        except Exception as e:
            print(f"  ❌ CSV 讀取失敗：{e}")
    if codes_str:
        extra = [c.strip() for c in codes_str.split(",") if c.strip()]
        codes = list(dict.fromkeys(codes + extra))
    return codes


# ──────────────────────────────────────────────
# 主程式
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="台股日當沖即時系統（Shioaji 推送）")
    parser.add_argument("--codes",      type=str,   default=None,
                        help="股票代號，逗號分隔，如 2330,6209")
    parser.add_argument("--csv",        type=str,   default=None,
                        help="從選股 CSV 載入候選股（--csv 五策略選股_20260416.csv）")
    parser.add_argument("--min-hit",    type=int,   default=1,
                        help="策略命中門檻，預設 1，建議設 2")
    parser.add_argument("--stop-loss",  type=float, default=DEFAULT_STOP,
                        help=f"停損門檻（%%），預設 {DEFAULT_STOP}")
    parser.add_argument("--vol-mult",   type=float, default=VOL_MULT,
                        help=f"爆量倍數，預設 {VOL_MULT}")
    parser.add_argument("--refresh",    type=int,   default=5,
                        help="畫面刷新間隔（秒），預設 5（Shioaji 推送模式下僅影響畫面更新頻率）")
    parser.add_argument("--no-notify",  action="store_true",
                        help="關閉 Windows 通知（只在終端機顯示）")
    parser.add_argument("--no-sound",   action="store_true",
                        help="關閉聲音提示")
    parser.add_argument("--ws-port",    type=int,   default=8765,
                        help="WebSocket server 埠號，預設 8765（0 = 關閉）")
    args = parser.parse_args()

    global _NOTIFY_ENABLED, _SOUND_ENABLED
    _NOTIFY_ENABLED = not args.no_notify
    _SOUND_ENABLED  = not args.no_sound

    # 載入候選股
    codes = load_codes(args.csv, args.codes)
    if not codes:
        print("  ❌ 請指定 --codes 或 --csv")
        sys.exit(1)

    print("\n" + "═"*65)
    print(f"  🔴 台股日當沖即時系統")
    print("═"*65)
    print(f"  監控股票：{len(codes)} 檔")
    print(f"  命中門檻：≥{args.min_hit} 個策略")
    print(f"  停損設定：{args.stop_loss}%")
    print(f"  爆量倍數：{args.vol_mult}x")
    print(f"  畫面刷新：每 {args.refresh} 秒")
    ws_port_str = str(args.ws_port) if args.ws_port else "停用"
    print(f"  WebSocket：{ws_port_str}")
    print("═"*65 + "\n")

    # 0. 啟動 WebSocket server（可選）
    broadcaster = WsBroadcaster(port=args.ws_port) if args.ws_port else None
    if broadcaster:
        broadcaster.start()

    # 1. 連線 Shioaji（提前到 fetch_history 前，讓 kbars 可用）
    api_key    = os.environ.get("SJ_API_KEY", "").strip()
    secret_key = os.environ.get("SJ_SECRET_KEY", "").strip()
    if not api_key:
        print("\n  ❌ 找不到環境變數 SJ_API_KEY")
        print("     請在 PowerShell 執行：")
        print("     [System.Environment]::SetEnvironmentVariable(\"SJ_API_KEY\", \"你的KEY\", \"User\")")
        print("     設定後重新開啟 PowerShell 再執行此程式")
        sys.exit(1)
    if not secret_key:
        print("\n  ❌ 找不到環境變數 SJ_SECRET_KEY")
        print("     請在 PowerShell 執行：")
        print("     [System.Environment]::SetEnvironmentVariable(\"SJ_SECRET_KEY\", \"你的KEY\", \"User\")")
        print("     設定後重新開啟 PowerShell 再執行此程式")
        sys.exit(1)

    lq = LiveQuote()
    if not lq.login(api_key, secret_key):
        sys.exit(1)

    # 2. 下載歷史資料（永豐金 kbars 主源，TWSE 備援），預算技術指標基準
    hist = fetch_history(codes, api=lq.get_api())
    if not hist:
        print("  ❌ 無法取得歷史資料（kbars 與 TWSE 均失敗）")
        print("     請確認網路可連線至 www.twse.com.tw")
        sys.exit(1)

    print(f"\n  ⚙️  預算技術指標基準...")
    bases = {}
    for code, df in hist.items():
        b = build_base(df)
        if b:
            bases[code] = b
    print(f"  ✅ {len(bases)} 檔基準預算完成")

    # 3. 訂閱即時 Tick
    lq.subscribe(list(bases.keys()), bases)

    # 4. 部位管理
    pm     = PositionManager(stop_loss_pct=args.stop_loss)
    alerts = []
    scan_n = 0

    # 5. 主迴圈（畫面刷新，資料由 Shioaji Tick callback 自動更新）
    try:
        while True:
            now = datetime.datetime.now()
            if not (TRADE_START <= now.time() <= TRADE_END):
                os.system("cls" if os.name == "nt" else "clear")
                print(f"  ⏰ 非交易時段（{now.strftime('%H:%M:%S')}），等待開盤...")
                time.sleep(30)
                continue

            scan_n += 1
            all_quotes = lq.get_all()

            # 計算訊號並更新部位損益
            signals = {}
            for code, base in bases.items():
                q      = all_quotes.get(code, {})
                price  = q.get("price", 0)
                open_p = q.get("open", base.get("open_price", 0))
                high_p = q.get("high", 0)
                low_p  = q.get("low", 0)
                prev_cl = base.get("prev_close", 0)
                # 漲跌幅即時計算（每次刷新從現價重算，非快照殘值）
                if price > 0 and prev_cl > 0:
                    chg = round((price - prev_cl) / prev_cl * 100, 2)
                else:
                    chg = q.get("chg_pct", 0)

                if price > 0:
                    pm.update_price(code, price)

                sigs = check_signals(base, price, open_p, chg, args.vol_mult,
                                     high_p=high_p, low_p=low_p)
                # 把即時重算的漲跌幅寫回 quote（供 render 顯示正確值）
                q_display = {**q, "chg_pct": chg}
                signals[code] = {"sigs": sigs, "quote": q_display}

                name = q.get("name", code)

                # ── 停損通知 ──────────────────────────────
                for pos in pm.get_positions():
                    if pos["code"] == code and pos.get("status") == "停損":
                        notify_stop(code, name,
                                    pos["direction"], pos["entry"], price)

                # ── 訊號狀態機：只在訊號集合有意義變化時警報 ──
                if price > 0:
                    alerted, alert_msg = check_and_alert(
                        code, name, sigs, price, args.min_hit
                    )
                    if alerted and alert_msg:
                        alerts.append(alert_msg)

                # ── 漲停/跌停出場提醒 ─────────────────────────
                if price > 0:
                    long_h  = sum(1 for k, v in sigs.items() if v and not k.endswith("S"))
                    short_h = sum(1 for k, v in sigs.items() if v and k.endswith("S"))
                    hit_n   = max(long_h, short_h)
                    dir_n   = "多" if long_h >= short_h else "空"
                    # prev_cl 已在上方由即時計算區段設定

                    if hit_n >= args.min_hit and prev_cl > 0 and code not in _notified_limits:
                        if dir_n == "多":
                            lup = calc_limit_up(prev_cl)
                            if price >= lup:
                                ts = now.strftime("%H:%M:%S")
                                alerts.append(
                                    f"{ts} 🚀 {code} {name} 漲停 {price:.2f}"
                                    f"（出場提醒，停損價 {lup:.2f}）"
                                )
                                notify_limit(code, name, "多", lup, price)
                        else:
                            ldn = calc_limit_down(prev_cl)
                            if price <= ldn:
                                ts = now.strftime("%H:%M:%S")
                                alerts.append(
                                    f"{ts} 🔻 {code} {name} 跌停 {price:.2f}"
                                    f"（出場提醒，跌停價 {ldn:.2f}）"
                                )
                                notify_limit(code, name, "空", ldn, price)

            # 渲染畫面
            render(
                positions=pm.get_positions(),
                signals=signals,
                scan_n=scan_n,
                alerts=alerts,
                summary=pm.summary(),
                min_hit=args.min_hit,
            )

            # 廣播至 Android App
            if broadcaster:
                broadcaster.broadcast(
                    _build_ws_payload(signals, pm, alerts, scan_n, args.min_hit)
                )

            time.sleep(args.refresh)

    except KeyboardInterrupt:
        print("\n\n  已停止。")
        s = pm.summary()
        if s["trades"] > 0:
            print(f"\n  今日交易紀錄：{s['trades']} 筆  勝率 {s['win']/s['trades']*100:.0f}%  "
                  f"總損益 {s['total_pnl']:+.2f}%")
            for h in pm.get_history():
                print(f"    {h['code']} {h['name']} {h['direction']} "
                      f"進 {h['entry']} → 出 {h.get('exit', '-')} "
                      f"({h.get('pnl_pct', 0):+.2f}%) [{h.get('reason','')}]")
    finally:
        lq.logout()


if __name__ == "__main__":
    main()
