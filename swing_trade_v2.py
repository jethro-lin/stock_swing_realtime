"""
台股隔日沖選股與回測系統 - swing_trade.py
==========================================
功能：
  1. 選股   -- 收盤後掃描，輸出明日候選股（終端機 + CSV）
  2. 回測   -- 對歷史資料跑策略，計算勝率 / 期望值 / 最大回撤
  3. 歷史查詢 -- 從 SQLite 查詢過去選股與實際損益

策略（三十六個，原有十六個 + 新增二十個）：
  原有策略：
    A多 均線突破+爆量   AS空 均線死亡+爆量
    B多 開盤跳空向上   BS空 開盤跳空向下
    C多 RSI超賣反彈    CS空 RSI超買反轉
    D多 突破近5日高點  DS空 跌破前低
    E多 強勢連漲       ES空 弱勢連跌
  均量類：
    F多 均量擴張+爆量上漲   FS空 均量萎縮+爆量下跌
    G多 縮量後爆量上漲      GS空 縮量後爆量下跌
  K棒型態類：
    H多 鎚子K（下影線≥2×實體）   HS空 射擊之星（上影線≥2×實體）
    I多 吞噬陽線                  IS空 吞噬陰線
  技術指標類（新增）：
    J多 MACD黃金交叉             JS空 MACD死亡交叉
    K多 布林下軌反彈             KS空 布林上軌反壓
    L多 KD超賣黃金交叉(<30)      LS空 KD超買死亡交叉(>70)
    M多 威廉%R超賣反彈(<-80)     MS空 威廉%R超買回落(>-20)
    N多 多頭排列回測MA5站回      NS空 空頭排列反彈MA5跌破
  進階K棒型態（新增）：
    O多 晨星三K棒               OS空 黃昏之星三K棒
    P多 紅三兵                  PS空 黑三兵
    Q多 Inside Bar向上突破      QS空 Inside Bar向下跌破
  均值回歸（新增）：
    R多 BIAS乖離率<-8%反彈      RS空 BIAS乖離率>+8%回落

進出場邏輯（隔日沖）：
  進場：今日收盤訊號成立 → 明日開盤價買入（或融券賣出）
  出場：明日收盤價
  停損：若設定 --stop-loss，用明日 Low（多）/ High（空）估算觸損（估算值）

SQLite 儲存：
  - swing_trade.db（與腳本同目錄）
  - 每次選股結果自動存入 scans 資料表
  - 回測結果存入 backtests 資料表
  - 可跨日查詢歷史選股與實際走勢

用法：
  # 選股（今日收盤後執行）
  python swing_trade.py --scan --min-hit 2 --save

  # 回測（60 日）
  python swing_trade.py --backtest --days 60 --min-hit 2 --stop-loss 1.5

  # 回測（30 日，比較兩個停損設定）
  python swing_trade.py --backtest --days 30 --stop-loss 0
  python swing_trade.py --backtest --days 30 --stop-loss 1.5

  # 查詢歷史選股
  python swing_trade.py --history --limit 10

  # 查詢特定股票歷史
  python swing_trade.py --history --code 2330
"""

import os
import sys
import math
import time
import sqlite3
import argparse
import datetime
import unicodedata

import numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests

try:
    import certifi
except ImportError:
    certifi = None

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

try:
    import twstock
    HAS_TWSTOCK = True
except ImportError:
    HAS_TWSTOCK = False

try:
    import shioaji as sj
    HAS_SHIOAJI = True
except ImportError:
    HAS_SHIOAJI = False


# ══════════════════════════════════════════════
# 常數
# ══════════════════════════════════════════════
DB_PATH      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "swing_trade.db")
TRADE_COST   = 0.00435   # 手續費 0.1425%×2 + 證交稅 0.15%
DEFAULT_DAYS = 60
RSI_PERIOD   = 14
VOL_MULT     = 1.5
MIN_AVG_VOL  = 3000      # 5 日均量最低門檻（張）

STRATEGY_NAMES = {
    # ── 原有十個策略 ──
    "A":  "多 均線突破+爆量",   "AS": "空 均線死亡+爆量",
    "B":  "多 開盤跳空向上",    "BS": "空 開盤跳空向下",
    "C":  "多 RSI超賣反彈",     "CS": "空 RSI超買反轉",
    "D":  "多 突破近5日高點",   "DS": "空 跌破近5日低點",
    "E":  "多 強勢連漲",        "ES": "空 弱勢連跌",
    # ── 均量類 ──
    "F":  "多 均量擴張+上漲",   "FS": "空 均量萎縮+下跌",
    "G":  "多 縮量後爆量上漲",  "GS": "空 縮量後爆量下跌",
    # ── K棒型態 ──
    "H":  "多 鎚子K",           "HS": "空 射擊之星",
    "I":  "多 吞噬陽線",        "IS": "空 吞噬陰線",
    # ── 技術指標類（新增）──
    "J":  "多 MACD黃金交叉",    "JS": "空 MACD死亡交叉",
    "K":  "多 布林下軌反彈",    "KS": "空 布林上軌反壓",
    "L":  "多 KD超賣黃金交叉",  "LS": "空 KD超買死亡交叉",
    "M":  "多 威廉%R超賣反彈",  "MS": "空 威廉%R超買回落",
    "N":  "多 多頭排列回測站回", "NS": "空 空頭排列反彈跌破",
    # ── 進階K棒型態（新增）──
    "O":  "多 晨星三K棒",       "OS": "空 黃昏之星三K棒",
    "P":  "多 紅三兵",          "PS": "空 黑三兵",
    "Q":  "多 InsideBar突破",   "QS": "空 InsideBar跌破",
    # ── 均值回歸（新增）──
    "R":  "多 BIAS超跌反彈",    "RS": "空 BIAS超漲回落",
}

# 預先建立策略順序索引（向量化與多執行緒共用）
_STRAT_KEYS = list(STRATEGY_NAMES.keys())            # ['A','AS','B','BS',...]
_IS_SHORT   = [k.endswith("S") for k in _STRAT_KEYS] # True 表示空方策略
_N_STRATS   = len(_STRAT_KEYS)


# ══════════════════════════════════════════════
# 組合預設清單（--preset 用）
# 新增自訂 preset：在 COMBO_PRESETS 加入一行即可
# ══════════════════════════════════════════════
COMBO_PRESETS: dict[str, list[str]] = {
# ── 空方 ────────────────────────────────────
    "short1": [
        "BS+FS+GS",   # 75.0%  EV+2.254%  n=36
        "AS+BS+GS",   # 72.7%  EV+2.555%  n=88
        "BS+DS+GS",   # 67.0%  EV+2.145%  n=100
        "BS+GS",      # 64.8%  EV+1.948%  n=108
        "BS+ES+GS",   # 60.0%  EV+0.340%  n=5  (⚠️少)
        "BS+ES+FS",   # 57.8%  EV+0.533%  n=64
        "AS+DS+GS",   # 56.5%  EV+0.850%  n=216
        "AS+BS+FS",   # 54.5%  EV+0.727%  n=442
        "AS+BS+DS",   # 53.0%  EV+0.634%  n=1118
        "AS+BS",      # 52.6%  EV+0.603%  n=1308
    ],
    # ── 多方 ─────────────────────────────────────
    "long1": [
        "B+E+F",   # 41.2%  EV-0.554%  n=413
        "B+E",     # 40.9%  EV-0.606%  n=711
        "B+D+E",   # 40.1%  EV-0.705%  n=568
        "B+C+D",   # 39.6%  EV-0.500%  n=164
        "A+B+E",   # 39.6%  EV-0.697%  n=626
        "B+D+F",   # 39.0%  EV-0.772%  n=1692
        "B+C",     # 38.1%  EV-0.540%  n=462
        "B+F",     # 37.9%  EV-0.727%  n=2190
        "A+B+F",   # 37.7%  EV-0.763%  n=2037
        "A+B+D",   # 37.3%  EV-0.815%  n=3217
        "B+D",     # 37.2%  EV-0.778%  n=3873
        "C+D+F",   # 37.0%  EV-0.475%  n=46
        "D+E+F",   # 36.6%  EV-0.664%  n=1085
        "A+B",     # 36.3%  EV-0.805%  n=4165
    ],
    # ── 空方（v2 新版，含 J~R 策略，90日回測 2026-04）──────────
    "short2": [
        # ▸ BS 錨點策略群（跳空向下 + 超買確認）
        "BS+MS+RS",   # 80.0%  EV+2.797%  n=30
        "BS+MS",      # 75.0%  EV+2.561%  n=44
        "BS+JS",      # 82.4%  EV+2.287%  n=17
        "BS+RS",      # 78.1%  EV+2.420%  n=32
        "BS+KS",      # 70.8%  EV+2.132%  n=24
        "BS+CS",      # 65.0%  EV+1.388%  n=20
        "BS+FS",      # 59.4%  EV+1.474%  n=32
        # ▸ FS/CS 策略群（量縮/RSI 超買）
        "CS+FS",      # 66.7%  EV+2.682%  n=15
        "FS+MS+RS",   # 70.6%  EV+2.123%  n=17
        "FS+RS",      # 72.7%  EV+1.728%  n=22
        "FS+MS",      # 63.9%  EV+1.380%  n=36
        # ▸ 純超買確認群（無跳空錨點，樣本較大）
        "IS+KS+RS",   # 59.2%  EV+1.250%  n=49
        "KS+MS+RS",   # 54.9%  EV+0.550%  n=384  ← 樣本最穩定
        "LS+MS+RS",   # 55.3%  EV+0.539%  n=190
    ],
    # ── 多方（v2 新版，含 J~R 策略，90日回測 2026-04）──────────
    "long2": [
        # ▸ 動能突破類
        "B+C+D",      # 63.6%  EV+1.633%  n=22
        "B+M+O",      # 54.5%  EV+1.027%  n=11
        "B+C",        # 48.9%  EV+0.589%  n=47
        "B+M",        # 51.2%  EV+0.369%  n=82
        "B+D+M",      # 52.6%  EV+0.429%  n=19
        # ▸ 超跌多重確認類
        "L+R",        # 60.0%  EV+1.566%  n=35
        "L+M+R",      # 57.6%  EV+1.440%  n=33
        "K+L+M",      # 47.6%  EV+1.050%  n=21
        "K+L",        # 47.8%  EV+0.992%  n=23
        "C+R",        # 55.2%  EV+0.606%  n=29
        "C+M+R",      # 53.8%  EV+0.324%  n=26
        # ▸ 大樣本穩健型
        "M+R",        # 49.5%  EV+0.066%  n=208
    ],
}

# ── all2：long2 + short2 + 本次回測所有正EV且樣本充足的補充組合 ────
# （long2 / short2 保留精選核心；all2 擴展為完整正EV清單，掃股用）
_long2_extra = [
    # 動能 + 超跌 補充
    "C+L+R",      # 60.0%  EV+1.529%  n=10
    "F+N+O",      # 52.6%  EV+0.819%  n=19
    "B+C+M",      # 51.7%  EV+0.269%  n=29
    "B+G",        # 40.7%  EV+0.132%  n=27
    "B+L",        # 50.0%  EV+0.093%  n=26
    "M+Q",        # 46.2%  EV+0.078%  n=13
    "B+L+M",      # 50.0%  EV+0.063%  n=24
    "D+N+O",      # 44.4%  EV+0.017%  n=45
    "A+D+I",      # 47.5%  EV+0.016%  n=40
]
_short2_extra = [
    # BS 補充
    "BS+CS+JS",   # 80.0%  EV+2.433%  n=10
    "BS+KS+RS",   # 69.2%  EV+2.413%  n=13
    "BS+KS+MS",   # 66.7%  EV+2.165%  n=21
    "BS+DS+FS",   # 50.0%  EV+0.870%  n=12
    # FS/CS 補充
    "CS+FS+MS",   # 70.0%  EV+2.099%  n=10
    "FS+JS",      # 64.3%  EV+1.545%  n=14
    "FS+OS",      # 81.8%  EV+1.145%  n=11
    # 純超買確認補充
    "KS+LS+OS",   # 80.0%  EV+1.641%  n=10
    "KS+OS+RS",   # 66.7%  EV+1.486%  n=12
    "IS+KS+LS",   # 73.7%  EV+1.305%  n=19
    "MS+OS+RS",   # 63.2%  EV+1.146%  n=38
    "JS+MS",      # 58.1%  EV+0.802%  n=43
    "LS+MS+OS",   # 55.8%  EV+0.768%  n=43
    "AS+MS",      # 53.8%  EV+0.724%  n=13
    "LS+OS+RS",   # 58.3%  EV+0.674%  n=24
    "CS+JS+MS",   # 60.9%  EV+0.551%  n=23
]
COMBO_PRESETS["all2"] = (
    COMBO_PRESETS["long2"]  + _long2_extra +
    COMBO_PRESETS["short2"] + _short2_extra
)


def resolve_preset(preset: str) -> list[str]:
    """
    將 --preset 名稱展開成組合清單。
    不存在時印出可用清單並回傳空 list。
    """
    key = preset.strip().lower()
    if key in COMBO_PRESETS:
        return COMBO_PRESETS[key]
    print(f"  ❌ 找不到 preset '{preset}'，可用清單：")
    for name, combos in COMBO_PRESETS.items():
        print(f"     {name}: {', '.join(combos)}")
    return []


# ══════════════════════════════════════════════
# 漲停 / 跌停價計算（台股最小升降單位）
# ══════════════════════════════════════════════
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


# ══════════════════════════════════════════════
# SQLite 資料庫
# ══════════════════════════════════════════════
def init_db() -> sqlite3.Connection:
    """初始化資料庫，建立資料表（如不存在）"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS scans (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_date   TEXT NOT NULL,          -- 選股日期 YYYY-MM-DD
        code        TEXT NOT NULL,
        name        TEXT,
        close       REAL,
        chg_pct     REAL,
        vol_k       INTEGER,
        vol_ratio   TEXT,
        strategies  TEXT,                   -- 命中策略，逗號分隔
        hit_count   INTEGER,
        direction   TEXT,                   -- 多 / 空
        min_hit     INTEGER,
        -- 次日實際走勢（事後填入）
        next_open   REAL,
        next_close  REAL,
        next_high   REAL,
        next_low    REAL,
        pnl_pct     REAL,                   -- 實際損益%（次日收盤）
        created_at  TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS backtests (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date      TEXT NOT NULL,
        strategy      TEXT NOT NULL,
        days          INTEGER,
        min_hit       INTEGER,
        stop_loss     REAL,
        signals       INTEGER,
        win_rate      REAL,
        avg_win       REAL,
        avg_loss      REAL,
        expectancy    REAL,
        max_loss      REAL,
        stop_rate     REAL,
        created_at    TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE INDEX IF NOT EXISTS idx_scans_date ON scans(scan_date);
    CREATE INDEX IF NOT EXISTS idx_scans_code ON scans(code);
    """)
    conn.commit()
    return conn


def save_scan(conn: sqlite3.Connection, scan_date: str, rows: list, min_hit: int):
    """儲存選股結果到 scans 資料表"""
    conn.executemany("""
        INSERT INTO scans
            (scan_date, code, name, close, chg_pct, vol_k, vol_ratio,
             strategies, hit_count, direction, min_hit)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, [
        (scan_date,
         r["代號"], r.get("名稱",""), r["收盤"], r["漲跌幅(%)"],
         r["成交量(張)"], r["量/均量"], r["策略清單"],
         r["命中數"], r["方向"], min_hit)
        for r in rows
    ])
    conn.commit()


def save_backtest_csv(stats: list, days: int, min_hit: int,
                      stop_loss: float) -> str:
    """
    儲存回測結果到 CSV。
    檔名包含日期、天數、門檻、停損，方便比較多次回測。
    """
    date_str = datetime.date.today().strftime("%Y%m%d")
    sl_str   = f"sl{stop_loss}" if stop_loss > 0 else "nosl"
    fname    = f"回測結果_{date_str}_d{days}_hit{min_hit}_{sl_str}.csv"
    df = pd.DataFrame(stats)
    # 加入執行資訊欄
    df.insert(0, "回測日期", datetime.date.today().isoformat())
    df.insert(1, "回測天數", days)
    df.insert(2, "命中門檻", min_hit)
    df.insert(3, "停損(%)",  stop_loss)
    df.to_csv(fname, index=False, encoding="utf-8-sig")
    return fname


def query_history(conn: sqlite3.Connection, code: str = None,
                  limit: int = 20) -> pd.DataFrame:
    """查詢歷史選股記錄"""
    if code:
        df = pd.read_sql(
            "SELECT scan_date,code,name,close,chg_pct,strategies,direction,pnl_pct "
            "FROM scans WHERE code=? ORDER BY scan_date DESC LIMIT ?",
            conn, params=(code, limit)
        )
    else:
        df = pd.read_sql(
            "SELECT scan_date,code,name,close,chg_pct,strategies,direction,hit_count,pnl_pct "
            "FROM scans ORDER BY scan_date DESC, hit_count DESC LIMIT ?",
            conn, params=(limit,)
        )
    return df


# ══════════════════════════════════════════════
# 股票名稱對照
# ══════════════════════════════════════════════
_NAME_MAP: dict = {}

def get_name(code: str) -> str:
    global _NAME_MAP
    if not _NAME_MAP and HAS_TWSTOCK:
        try:
            _NAME_MAP = {c: getattr(s, "name", c)
                         for c, s in twstock.codes.items() if c.isdigit()}
        except Exception:
            pass
    return _NAME_MAP.get(str(code), code)


# ══════════════════════════════════════════════
# 資料新鮮度檢查（共用）
# ══════════════════════════════════════════════
def _check_data_freshness(results: dict, source: str = ""):
    """檢查並印出資料新鮮度警告"""
    last_dates = []
    for df in results.values():
        try:
            last_dates.append(df.index[-1].date())
        except Exception:
            pass
    if not last_dates:
        return
    data_date = max(last_dates)
    today_d   = datetime.date.today()
    delta     = (today_d - data_date).days
    # 台股週一至週五開市；週一 delta==3 表示資料是上週五，屬正常
    is_weekend_gap = (today_d.weekday() == 0 and delta == 3)
    if delta == 0:
        print(f"  📅 資料截至：{data_date}（今日，✅ 最新）")
    elif is_weekend_gap or delta <= 1:
        print(f"  📅 資料截至：{data_date}（上個交易日，✅ 正常）")
    else:
        src_hint = f"{source} 尚未更新今日收盤" if source else "資料可能過時"
        print(f"  ⚠️  資料截至：{data_date}（{delta} 天前）"
              f"— {src_hint}，選股結果為 {data_date} 的訊號！")


# ══════════════════════════════════════════════
# 資料下載
# ══════════════════════════════════════════════
def _get_top_codes(top_n: int = 300) -> list:
    """從 TWSE 取得成交量前 N 名的股票代號（三層 fallback）"""
    # TWSE 憑證有 Missing Subject Key Identifier 問題
    # 優先用 certifi，失敗時改用 verify=False
    ssl_opt = certifi.where() if certifi else True
    ssl_fallback = False  # 若 certifi 仍失敗，切換到 verify=False

    # Layer 1：TWSE 盤後 CSV（最穩定）
    try:
        url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
        for verify in [ssl_opt, False]:  # 先用 certifi，失敗再用 verify=False
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"},
                                 timeout=20, verify=verify)
                break
            except Exception:
                if verify is False:
                    raise
                continue
        df  = pd.read_csv(__import__("io").StringIO(r.text))
        # 欄位：證券代號, 證券名稱, 成交股數, ...
        code_col = next((c for c in df.columns if "代號" in c or "code" in c.lower()), df.columns[0])
        vol_col  = next((c for c in df.columns if "成交" in c and "股" in c), None)
        if vol_col:
            df[vol_col] = pd.to_numeric(df[vol_col].astype(str).str.replace(",",""), errors="coerce")
            df = df.dropna(subset=[vol_col])
            df = df[df[code_col].astype(str).str.match(r"^\d{4}$")]
            df = df.sort_values(vol_col, ascending=False)
            codes = df[code_col].astype(str).str.strip().head(top_n).tolist()
            if codes:
                print(f"  ✅ TWSE CSV：取得 {len(codes)} 檔")
                return codes
    except Exception as e:
        print(f"  ⚠️  TWSE CSV 失敗：{e}")

    # Layer 2：TWSE JSON API
    try:
        url  = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?type=ALLBUT0999&response=json"
        for verify in [ssl_opt, False]:
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"},
                                 timeout=20, verify=verify)
                break
            except Exception:
                if verify is False:
                    raise
                continue
        data = r.json()
        rows = data.get("data9", data.get("data5", data.get("data", [])))
        codes = []
        for row in rows:
            try:
                code = str(row[0]).strip()
                vol  = int(str(row[2]).replace(",", ""))
                if code.isdigit() and len(code) == 4:
                    codes.append((code, vol))
            except Exception:
                continue
        if codes:
            codes.sort(key=lambda x: x[1], reverse=True)
            result = [c for c, _ in codes[:top_n]]
            print(f"  ✅ TWSE JSON：取得 {len(result)} 檔")
            return result
    except Exception as e:
        print(f"  ⚠️  TWSE JSON 失敗：{e}")

    # Layer 3：twstock 內建清單（以常見大型股優先排序）
    if HAS_TWSTOCK:
        try:
            # 優先選常見上市股（避免冷門股 yfinance 下載失敗）
            priority = ["2330","2317","2454","2382","2412","2308","2303","3711",
                        "2881","2882","2886","2891","2892","2884","2885","2880",
                        "1301","1303","1326","2002","2886","6505","5880","2207"]
            all_codes = [c for c in twstock.codes if c.isdigit() and len(c) == 4]
            # 把 priority 排前面，其餘補齊
            rest   = [c for c in all_codes if c not in priority]
            result = (priority + rest)[:top_n]
            print(f"  ✅ twstock 備援：取得 {len(result)} 檔（優先大型股）")
            return result
        except Exception:
            pass

    print("  ❌ 三層 fallback 均失敗，請使用 --codes 指定股票")
    return []


def fetch_data(codes: list, days: int = DEFAULT_DAYS) -> dict:
    """下載歷史 K 線資料"""
    if not HAS_YF:
        print("  ❌ 請安裝：pip install yfinance")
        return {}

    # 盤中保護：yfinance end 設為今日即可；dropna 本來就會過濾掉盤中不完整列
    _tz_tw    = datetime.timezone(datetime.timedelta(hours=8))
    _now_tw   = datetime.datetime.now(_tz_tw)
    _open_tw  = _now_tw.replace(hour=9,  minute=0,  second=0, microsecond=0)
    _close_tw = _now_tw.replace(hour=13, minute=30, second=0, microsecond=0)
    if _open_tw <= _now_tw < _close_tw:
        print(f"  ⏰ 現在 {_now_tw.strftime('%H:%M')} 台灣時間，盤中執行"
              f"——使用昨日收盤訊號選股")

    buf     = days + 30
    end     = datetime.date.today() + datetime.timedelta(days=1)
    start   = datetime.date.today() - datetime.timedelta(days=buf)
    def _to_ticker(code: str) -> str:
        """判斷上市(.TW)或上櫃(.TWO)"""
        if HAS_TWSTOCK:
            try:
                s = twstock.codes.get(code)
                if s:
                    mtype = getattr(s, "market_type", "") or ""
                    if "OTC" in mtype or "上櫃" in mtype:
                        return f"{code}.TWO"
                    return f"{code}.TW"
            except Exception:
                pass
        # fallback：代號首字 4 或 0 通常是上櫃
        return f"{code}.TWO" if code.startswith(("4","0")) else f"{code}.TW"

    tickers  = [_to_ticker(c) for c in codes]
    # ticker → code 反查表
    t2c = {t: t.replace(".TW","").replace("O","") if t.endswith(".TWO")
           else t.replace(".TW","") for t in tickers}
    t2c = {t: t.replace(".TWO","").replace(".TW","") for t in tickers}
    # 抑制 yfinance 下市股票警告（如 "$3128.TW: possibly delisted"）
    import warnings, logging
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    warnings.filterwarnings("ignore", category=UserWarning)

    results = {}
    batch   = 50

    def _download_batch(sub_tickers, sub_t2c):
        """下載一批 ticker，回傳 {code: df}"""
        partial = {}
        try:
            raw = yf.download(sub_tickers, start=str(start), end=str(end),
                              group_by="ticker", auto_adjust=False,
                              progress=False, threads=True)
            for ticker in sub_tickers:
                code = sub_t2c.get(ticker, ticker.replace(".TWO","").replace(".TW",""))
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        if ticker in raw.columns.get_level_values(0):
                            df = raw[ticker].copy()
                        else:
                            continue
                    else:
                        df = raw.copy()
                    df.dropna(subset=["Close"], inplace=True)
                    if len(df) >= 22:
                        df["Vol_K"] = df["Volume"] / 1000
                        partial[code] = df
                except Exception:
                    pass
        except Exception as e:
            print(f"  ⚠️  批次下載失敗：{e}")
        return partial

    print(f"  📥 下載 {len(codes)} 檔資料（{start} ~ {end}）...")
    for i in range(0, len(tickers), batch):
        sub = tickers[i:i+batch]
        sub_t2c = {t: t2c[t] for t in sub if t in t2c}
        results.update(_download_batch(sub, sub_t2c))
        time.sleep(0.3)

    # ── 自動換 suffix 重試（修正 .TW / .TWO 誤判，如 4540 等）──
    missing = [c for c in codes if c not in results]
    if missing:
        def _alt_ticker(code: str) -> str:
            orig = _to_ticker(code)
            return f"{code}.TW" if orig.endswith(".TWO") else f"{code}.TWO"

        alt_tickers = [_alt_ticker(c) for c in missing]
        alt_t2c     = {t: t.replace(".TWO","").replace(".TW","") for t in alt_tickers}
        print(f"  🔄 換 suffix 重試 {len(missing)} 檔（.TW ↔ .TWO）：{', '.join(missing[:10])}"
              + ("..." if len(missing) > 10 else ""))
        for i in range(0, len(alt_tickers), batch):
            sub = alt_tickers[i:i+batch]
            sub_t2c = {t: alt_t2c[t] for t in sub}
            results.update(_download_batch(sub, sub_t2c))
            time.sleep(0.3)

    print(f"  ✅ 成功取得 {len(results)} 檔")
    _check_data_freshness(results, source="yfinance")
    return results


# ══════════════════════════════════════════════
# 永豐金 Shioaji 資料下載（混合模式）
# ══════════════════════════════════════════════
def fetch_data_sinopac(codes: list, days: int,
                       api_key: str, secret_key: str) -> dict:
    """
    混合模式：
      歷史資料（D-1 以前） → yfinance 批次下載（速度快）
      今日 OHLCV          → 永豐金 api.snapshots()（即時、準確）

    原因：api.kbars() 歷史日K需要較高 API 權限，一般帳號不開放；
          但 snapshots 即時報價是基本功能，盤後仍可取到今日完整收盤。

    需求：pip install shioaji  ＋ 永豐金 API 金鑰（SJ_API_KEY / SJ_SECRET_KEY）
    """
    if not HAS_SHIOAJI:
        print("  ❌ 請先安裝：pip install shioaji")
        return {}

    # ── 台灣時間判斷 ─────────────────────────────
    _tz_tw       = datetime.timezone(datetime.timedelta(hours=8))
    _now_tw      = datetime.datetime.now(_tz_tw)
    _today_tw    = _now_tw.date()
    _open_tw     = _now_tw.replace(hour=9,  minute=0,  second=0, microsecond=0)
    _close_tw    = _now_tw.replace(hour=13, minute=30, second=0, microsecond=0)
    _market_open = (_open_tw <= _now_tw < _close_tw)

    # ── 第一步：yfinance 下載歷史資料 ─────────────
    print(f"  📥 [1/2] yfinance 下載歷史資料...")
    results = fetch_data(codes, days)
    if not results:
        return {}

    # ── 第二步：永豐金 snapshots 補上今日收盤 ───────
    print(f"  📡 [2/2] 永豐金 snapshots 補上今日 OHLCV...")
    if _market_open:
        print(f"  ⏰ 現在 {_now_tw.strftime('%H:%M')} 台灣時間，盤中執行"
              f"——snapshots 為即時盤中報價，非收盤價，今日訊號僅供參考")

    try:
        api = sj.Shioaji()
        api.login(
            api_key=api_key,
            secret_key=secret_key,
            contracts_timeout=30000,
        )
        print(f"  ✅ 永豐金登入成功（api_key: {api_key[:6]}...）")

        # 等待合約就緒
        for _w in range(31):
            try:
                if api.Contracts.Stocks["2330"] is not None:
                    break
            except Exception:
                pass
            time.sleep(1)

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

        # 收集有歷史資料的股票合約
        contracts, c2code = [], {}
        for code in list(results.keys()):
            c = _get_contract(code)
            if c is not None:
                contracts.append(c)
                c2code[c.code] = code

        if contracts:
            # ── 先記錄 yfinance 今日的量（對比用）──────────────
            yf_today_vol = {}
            for code, df in results.items():
                if len(df) > 0 and df.index[-1].date() == _today_tw:
                    yf_today_vol[code] = float(df["Vol_K"].iloc[-1])

            # ── 分批取 snapshots（每批最多 200）─────────────────
            updated    = 0
            zero_vol   = 0   # total_volume=0 的筆數（保留 yfinance）
            stale_codes = []  # snapshot 日期與今日不符的股票
            ratio_list = []  # (code, yf_vol, sj_vol) 量比記錄

            for bi in range(0, len(contracts), 200):
                batch = contracts[bi:bi+200]
                try:
                    snaps = api.snapshots(batch)
                    for snap in snaps:
                        code = c2code.get(snap.code, snap.code)
                        if code not in results:
                            continue
                        close = float(snap.close)
                        if close <= 0:
                            continue

                        # ── 從 snap.ts 取出 snapshot 的實際資料日期 ──────────
                        snap_ts = getattr(snap, "ts", None)
                        if snap_ts and snap_ts > 0:
                            snap_date = datetime.datetime.fromtimestamp(
                                snap_ts / 1e9, tz=_tz_tw
                            ).date()
                        else:
                            snap_date = _today_tw  # 無 ts 欄位時退回今日

                        # 記錄資料日期與執行日不符的股票（可能是昨日舊資料）
                        if snap_date != _today_tw:
                            stale_codes.append(
                                f"{code}（snap={snap_date}, 今日={_today_tw}）"
                            )

                        # snap.volume       = 最後一筆成交量（單筆，極小）← 錯誤欄位
                        # snap.total_volume = 當日累計總成交量（張）      ← 正確欄位
                        tv    = float(snap.total_volume) if snap.total_volume else 0.0
                        lv    = float(snap.volume)       if snap.volume       else 0.0
                        vol_k = tv or lv

                        if vol_k <= 0:
                            # Shioaji 無量資料 → 保留 yfinance 原始資料，不替換
                            zero_vol += 1
                            continue

                        # 記錄量比（用於後續診斷）
                        yf_v = yf_today_vol.get(code, 0)
                        ratio_list.append((code, yf_v, vol_k))

                        # ── 用 snap_date（真實日期）當 bar index ─────────────
                        today_bar = pd.DataFrame({
                            "Open":   [float(snap.open)],
                            "High":   [float(snap.high)],
                            "Low":    [float(snap.low)],
                            "Close":  [close],
                            "Volume": [vol_k * 1000],   # 張 → 股（與 yfinance 對齊）
                            "Vol_K":  [vol_k],
                        }, index=[pd.Timestamp(snap_date)])
                        df = results[code]
                        # 移除同日期的 yfinance 舊資料（若有），換成 Shioaji 即時
                        if len(df) > 0 and df.index[-1].date() == snap_date:
                            df = df.iloc[:-1]
                        results[code] = pd.concat([df, today_bar])
                        updated += 1
                except Exception as e:
                    print(f"  ⚠️  snapshots 批次失敗：{e}")

            status = "盤中即時" if _market_open else "今日收盤"
            print(f"  ✅ 永豐金 {status} 更新：{updated}/{len(contracts)} 檔"
                  f"  （零成交量跳過：{zero_vol} 檔）")

            # ── 資料日期不一致警告 ────────────────────────────────
            if stale_codes:
                print(f"\n  ⚠️  【資料日期異常】以下 {len(stale_codes)} 檔 snapshot 日期"
                      f"與今日（{_today_tw}）不符，訊號可能基於昨日舊資料：")
                for s in stale_codes[:10]:
                    print(f"     • {s}")
                if len(stale_codes) > 10:
                    print(f"     ...（共 {len(stale_codes)} 檔）")
                print(f"  💡 建議：收盤後 5 分鐘再執行，或改用 --datasource yfinance")

            # ── 診斷：Shioaji vs yfinance 量比分析 ───────────────
            if ratio_list:
                ratios_by_code = {code: (sj / yf) for code, yf, sj in ratio_list if yf > 0}
                ratios = sorted(ratios_by_code.values())
                if ratios:
                    median_r = ratios[len(ratios) // 2]
                    print(f"\n  🔬 Shioaji vs yfinance 量比（共 {len(ratios)} 檔）：")
                    print(f"     中位數={median_r:.3f}x  "
                          f"最小={ratios[0]:.3f}x  最大={ratios[-1]:.3f}x")
                    if 0.5 < median_r < 2.0:
                        print(f"     ✅ 量單位一致（中位數接近 1x）")
                    elif median_r > 500:
                        print(f"     ⚠️  Shioaji 量疑似為【股/Shares】，需除以 1000 才是張")
                    elif median_r < 0.05:
                        print(f"     ⚠️  Shioaji 量遠低於 yfinance，可能快照時間不對或欄位錯誤")

                    # ── 校正：以 Shioaji 為基準，校正歷史 yfinance 量 ──
                    # 每檔用個別量比；無量比資料的股票用中位數代替
                    calibrated = 0
                    for code, df in results.items():
                        factor = ratios_by_code.get(code, median_r)
                        if abs(factor - 1.0) < 0.001:
                            continue   # 幾乎相同，跳過
                        # 只校正歷史列（非今日）；今日已是 Shioaji 真實值
                        mask = df.index.date != _today_tw
                        if mask.any():
                            df.loc[mask, "Vol_K"]  = df.loc[mask, "Vol_K"]  * factor
                            df.loc[mask, "Volume"] = df.loc[mask, "Volume"] * factor
                            results[code] = df
                            calibrated += 1
                    print(f"     📐 歷史量校正完成：{calibrated} 檔"
                          f"（以 Shioaji 為基準，yfinance 歷史量 × 個別量比）")

                    # 找出量比最異常的 5 筆
                    ratio_list.sort(key=lambda x: abs((x[2]/x[1] if x[1] > 0 else 999) - 1), reverse=True)
                    print(f"\n     量比最異常的前 5 檔（Shioaji 與 yfinance 差異最大）：")
                    print(f"     {'代號':6s}  {'yfinance(張)':>12s}  {'Shioaji(張)':>12s}  {'比值':>8s}")
                    print(f"     {'─'*52}")
                    for code, yf_v, sj_v in ratio_list[:5]:
                        r = sj_v / yf_v if yf_v > 0 else float("inf")
                        print(f"     {code:6s}  {yf_v:>12.0f}  {sj_v:>12.0f}  {r:>8.2f}x")
                    print()
        else:
            print(f"  ⚠️  找不到可用合約，跳過今日更新")

        try:
            api.logout()
            print(f"  🔓 已登出永豐金 API")
        except Exception:
            pass

    except Exception as e:
        print(f"  ⚠️  永豐金連線失敗（{e}），僅使用 yfinance 歷史資料")

    _check_data_freshness(results, source="永豐金+yfinance")
    return results


# ══════════════════════════════════════════════
# 技術指標
# ══════════════════════════════════════════════
def _rsi(series: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    return 100 - 100 / (1 + gain / loss.replace(0, float("nan")))


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """回傳 (macd_line, signal_line) 兩個 Series"""
    ema_fast   = series.ewm(span=fast,   adjust=False).mean()
    ema_slow   = series.ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


def _bollinger(series: pd.Series, period: int = 20, n_std: float = 2.0):
    """回傳 (upper, mid, lower) 三個 Series"""
    mid   = series.rolling(period).mean()
    std   = series.rolling(period).std()
    return mid + n_std * std, mid, mid - n_std * std


def _stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                period: int = 9, smooth_k: int = 3, smooth_d: int = 3):
    """KD 隨機指標，回傳 (K, D) 兩個 Series"""
    hi  = high.rolling(period).max()
    lo  = low.rolling(period).min()
    rsv = ((close - lo) / (hi - lo).replace(0, float("nan")) * 100).fillna(50)
    k   = rsv.ewm(com=smooth_k - 1, adjust=False).mean()
    d   = k.ewm(com=smooth_d - 1, adjust=False).mean()
    return k, d


def _williams_r(high: pd.Series, low: pd.Series, close: pd.Series,
                period: int = 14) -> pd.Series:
    """Williams %R，值域 -100 ~ 0"""
    hi = high.rolling(period).max()
    lo = low.rolling(period).min()
    return ((hi - close) / (hi - lo).replace(0, float("nan")) * -100).fillna(-50)


def _check(df: pd.DataFrame, i: int,
           min_avg_vol: int, vol_mult: float) -> dict:
    """
    在第 i 日計算所有策略訊號。
    回傳 {A: bool, B: bool, ... ES: bool}
    """
    empty = {k: False for k in STRATEGY_NAMES}
    if i < 20:
        return empty

    win    = df.iloc[:i+1]
    today  = win.iloc[-1]
    prev   = win.iloc[-2] if len(win) >= 2 else today

    close   = float(today["Close"])
    open_p  = float(today["Open"])
    prev_cl = float(prev["Close"])
    vol_k   = float(today["Vol_K"])
    avg5    = float(win["Vol_K"].iloc[-6:-1].mean())
    chg_pct = (close - prev_cl) / prev_cl * 100 if prev_cl else 0

    if avg5 < min_avg_vol:
        return empty

    vol_ratio = vol_k / avg5 if avg5 > 0 else 0
    ma5   = float(win["Close"].iloc[-5:].mean())
    ma10  = float(win["Close"].iloc[-10:].mean())
    ma20  = float(win["Close"].iloc[-20:].mean())
    rsi   = _rsi(win["Close"], 14)
    rsi_t = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50
    rsi_p = float(rsi.iloc[-2]) if not pd.isna(rsi.iloc[-2]) else 50
    gap   = (open_p - prev_cl) / prev_cl * 100 if prev_cl else 0
    hi5   = float(win["High"].iloc[-6:-1].max())
    lo5   = float(win["Low"].iloc[-6:-1].min())

    three_up = all(
        float(win["Close"].iloc[-(j+1)]) > float(win["Open"].iloc[-(j+1)])
        for j in range(1, 4)
    ) if len(win) >= 4 else False
    three_dn = all(
        float(win["Close"].iloc[-(j+1)]) < float(win["Open"].iloc[-(j+1)])
        for j in range(1, 4)
    ) if len(win) >= 4 else False

    # ── 新策略計算材料 ──────────────────────────
    # 均量類：近5日均量 vs 近20日均量
    avg20    = float(win["Vol_K"].iloc[-21:-1].mean()) if len(win) >= 22 else avg5
    avg3     = float(win["Vol_K"].iloc[-4:-1].mean())  if len(win) >= 4  else avg5
    # 均量擴張：近5日均量 > 近20日均量 × 1.2
    vol_expand  = avg5 > avg20 * 1.2 if avg20 > 0 else False
    # 均量萎縮：近5日均量 < 近20日均量 × 0.8
    vol_shrink  = avg5 < avg20 * 0.8 if avg20 > 0 else False
    # 潮汐：連續3日縮量（近3日各自 < 前一日量）
    tide_shrink = all(
        float(win["Vol_K"].iloc[-(j+1)]) < float(win["Vol_K"].iloc[-(j+2)])
        for j in range(1, 4)
    ) if len(win) >= 5 else False

    # K棒型態類
    body     = abs(close - open_p)
    upper    = float(today["High"]) - max(close, open_p)
    lower    = min(close, open_p) - float(today["Low"])
    is_bull  = close > open_p   # 今日陽線
    is_bear  = close < open_p   # 今日陰線

    prev_body  = abs(float(prev["Close"]) - float(prev["Open"]))
    prev_bull  = float(prev["Close"]) > float(prev["Open"])
    prev_bear  = float(prev["Close"]) < float(prev["Open"])
    prev_close_p = float(prev["Close"])
    prev_open_p  = float(prev["Open"])

    # 鎚子K：下影線 ≥ 2×實體，上影線 ≤ 實體，前日收跌
    hammer      = (body > 0 and lower >= 2 * body and upper <= body
                   and chg_pct < 0 and prev_bear)
    # 射擊之星：上影線 ≥ 2×實體，下影線 ≤ 實體，前日收漲
    shoot_star  = (body > 0 and upper >= 2 * body and lower <= body
                   and chg_pct > 0 and prev_bull)
    # 吞噬陽線：今日陽線完全包覆前日陰線（今開 < 前收，今收 > 前開）
    engulf_bull = (is_bull and prev_bear
                   and open_p <= prev_close_p and close >= prev_open_p
                   and body > prev_body * 0.8)
    # 吞噬陰線：今日陰線完全包覆前日陽線（今開 > 前收，今收 < 前開）
    engulf_bear = (is_bear and prev_bull
                   and open_p >= prev_close_p and close <= prev_open_p
                   and body > prev_body * 0.8)

    # ── 技術指標類（新增）────────────────────────────────
    # J/JS: MACD 黃金/死亡交叉 (12/26/9)
    _macd_line, _macd_sig = _macd(win["Close"])
    macd_t    = float(_macd_line.iloc[-1])   if not pd.isna(_macd_line.iloc[-1])  else 0.0
    macd_p    = float(_macd_line.iloc[-2])   if len(_macd_line) >= 2 and not pd.isna(_macd_line.iloc[-2]) else 0.0
    msig_t    = float(_macd_sig.iloc[-1])    if not pd.isna(_macd_sig.iloc[-1])   else 0.0
    msig_p    = float(_macd_sig.iloc[-2])    if len(_macd_sig) >= 2 and not pd.isna(_macd_sig.iloc[-2])  else 0.0
    macd_golden = (macd_p < msig_p) and (macd_t > msig_t)
    macd_death  = (macd_p > msig_p) and (macd_t < msig_t)

    # K/KS: 布林通道下軌反彈 / 上軌反壓 (20,2)
    _bb_up, _bb_mid, _bb_lo = _bollinger(win["Close"])
    bb_lower_t  = float(_bb_lo.iloc[-1])  if not pd.isna(_bb_lo.iloc[-1])  else close
    bb_upper_t  = float(_bb_up.iloc[-1])  if not pd.isna(_bb_up.iloc[-1])  else close
    bb_lower_p  = float(_bb_lo.iloc[-2])  if len(_bb_lo) >= 2 and not pd.isna(_bb_lo.iloc[-2]) else bb_lower_t
    bb_upper_p  = float(_bb_up.iloc[-2])  if len(_bb_up) >= 2 and not pd.isna(_bb_up.iloc[-2]) else bb_upper_t
    bb_bounce    = (prev_close_p <= bb_lower_p) and (close > bb_lower_t)
    bb_rejection = (prev_close_p >= bb_upper_p) and (close < bb_upper_t)

    # L/LS: KD 超賣黃金交叉(<30) / 超買死亡交叉(>70)
    _kd_k, _kd_d = _stochastic(win["High"], win["Low"], win["Close"])
    kk_t = float(_kd_k.iloc[-1]) if not pd.isna(_kd_k.iloc[-1]) else 50.0
    kd_t = float(_kd_d.iloc[-1]) if not pd.isna(_kd_d.iloc[-1]) else 50.0
    kk_p = float(_kd_k.iloc[-2]) if len(_kd_k) >= 2 and not pd.isna(_kd_k.iloc[-2]) else 50.0
    kd_p = float(_kd_d.iloc[-2]) if len(_kd_d) >= 2 and not pd.isna(_kd_d.iloc[-2]) else 50.0
    kd_oversold_cross   = (kk_t < 30) and (kk_p < kd_p) and (kk_t > kd_t)
    kd_overbought_cross = (kk_t > 70) and (kk_p > kd_p) and (kk_t < kd_t)

    # M/MS: 威廉%R 超賣(<-80)反彈 / 超買(>-20)回落
    _wr  = _williams_r(win["High"], win["Low"], win["Close"])
    wr_t = float(_wr.iloc[-1]) if not pd.isna(_wr.iloc[-1]) else -50.0
    wr_p = float(_wr.iloc[-2]) if len(_wr) >= 2 and not pd.isna(_wr.iloc[-2]) else -50.0
    wr_oversold    = (wr_p < -80) and (wr_t > wr_p) and (chg_pct > 0)
    wr_overbought  = (wr_p > -20) and (wr_t < wr_p) and (chg_pct < 0)

    # N/NS: 均線多頭排列+昨收跌破ma5今收站回 / 空頭排列+昨收突破ma5今收跌破
    ma_bull_align  = (ma5 > ma10 > ma20)
    ma_bear_align  = (ma5 < ma10 < ma20)
    ma_pull_bull   = ma_bull_align and (prev_close_p < ma5) and (close >= ma5)
    ma_pull_bear   = ma_bear_align and (prev_close_p > ma5) and (close <= ma5)

    # ── 進階K棒型態（新增）────────────────────────────────
    # O/OS: 晨星 / 黃昏之星（三K棒）
    if len(win) >= 3:
        day1 = win.iloc[-3]
        day2 = win.iloc[-2]
        d1_c  = float(day1["Close"]); d1_o  = float(day1["Open"])
        d1_body = abs(d1_c - d1_o)
        d2_body = abs(float(day2["Close"]) - float(day2["Open"]))
        d1_mid  = (d1_c + d1_o) / 2
        morning_star = (
            d1_c < d1_o and d1_body > 0 and          # Day1 大陰線
            d2_body < d1_body * 0.4 and               # Day2 小實體（猶豫）
            is_bull and                               # Day3 今日陽線
            close > d1_mid and                        # Day3 收盤超過Day1中點
            body >= d1_body * 0.5                     # Day3 實體夠大
        )
        evening_star = (
            d1_c > d1_o and d1_body > 0 and          # Day1 大陽線
            d2_body < d1_body * 0.4 and               # Day2 小實體
            is_bear and                               # Day3 今日陰線
            close < d1_mid and                        # Day3 收盤低於Day1中點
            body >= d1_body * 0.5                     # Day3 實體夠大
        )
    else:
        morning_star = False
        evening_star = False

    # P/PS: 紅三兵 / 黑三兵（連3根，每根方向一致且收盤遞進）
    if len(win) >= 3:
        d1 = win.iloc[-3]; d2 = win.iloc[-2]
        d1_c2 = float(d1["Close"]); d1_o2 = float(d1["Open"])
        d2_c2 = float(d2["Close"]); d2_o2 = float(d2["Open"])
        three_soldiers = (
            d1_c2 > d1_o2 and d2_c2 > d2_o2 and is_bull and   # 三根都是陽線
            d2_c2 > d1_c2 and close > d2_c2 and               # 收盤逐步墊高
            d2_o2 >= d1_o2 and open_p >= d2_o2                 # 開盤不低於前根開盤
        )
        three_crows = (
            d1_c2 < d1_o2 and d2_c2 < d2_o2 and is_bear and   # 三根都是陰線
            d2_c2 < d1_c2 and close < d2_c2 and               # 收盤逐步下沉
            d2_o2 <= d1_o2 and open_p <= d2_o2                 # 開盤不高於前根開盤
        )
    else:
        three_soldiers = False
        three_crows    = False

    # Q/QS: Inside Bar 突破/跌破（前日為 Inside Bar，今日突破/跌破前前日高低）
    if len(win) >= 3:
        prev2      = win.iloc[-3]
        prev2_h    = float(prev2["High"]); prev2_l = float(prev2["Low"])
        prev_h_q   = float(prev["High"]);  prev_l_q = float(prev["Low"])
        is_inside  = (prev_h_q < prev2_h) and (prev_l_q > prev2_l)
        inside_breakout  = is_inside and (close > prev2_h) and (vol_ratio >= vol_mult)
        inside_breakdown = is_inside and (close < prev2_l) and (vol_ratio >= vol_mult)
    else:
        inside_breakout  = False
        inside_breakdown = False

    # ── 均值回歸（新增）──────────────────────────────────
    # R/RS: BIAS 乖離率（對 MA20）過大後反向
    bias          = (close - ma20) / ma20 * 100 if ma20 > 0 else 0
    bias_oversold   = (bias < -8) and (chg_pct > 0)
    bias_overbought = (bias > +8) and (chg_pct < 0)

    return {
        # 原有十個
        "A":  bool(ma5 > ma20 and vol_ratio >= vol_mult and chg_pct > 0),
        "AS": bool(ma5 < ma20 and vol_ratio >= vol_mult and chg_pct < 0),
        "B":  bool(gap >= 2.0 and vol_ratio >= 1.3 and chg_pct > 0),
        "BS": bool(gap <= -2.0 and vol_ratio >= 1.3 and chg_pct < 0),
        "C":  bool(rsi_p < 35 and rsi_t > rsi_p and close > ma5),
        "CS": bool(rsi_p > 65 and rsi_t < rsi_p and close < ma5),
        "D":  bool(close > hi5 and vol_ratio >= vol_mult),
        "DS": bool(close < lo5 and vol_ratio >= vol_mult),
        "E":  bool(three_up and ma5 > ma10 > ma20 and chg_pct > 0),
        "ES": bool(three_dn and ma5 < ma10 < ma20 and chg_pct < 0),
        # 均量類
        "F":  bool(vol_expand and vol_ratio >= vol_mult and chg_pct > 0),
        "FS": bool(vol_shrink and vol_ratio >= vol_mult and chg_pct < 0),
        "G":  bool(tide_shrink and vol_ratio >= vol_mult and chg_pct > 0),
        "GS": bool(tide_shrink and vol_ratio >= vol_mult and chg_pct < 0),
        # K棒型態
        "H":  bool(hammer),
        "HS": bool(shoot_star),
        "I":  bool(engulf_bull),
        "IS": bool(engulf_bear),
        # 技術指標類（新增）
        "J":  bool(macd_golden and chg_pct > 0),
        "JS": bool(macd_death  and chg_pct < 0),
        "K":  bool(bb_bounce),
        "KS": bool(bb_rejection),
        "L":  bool(kd_oversold_cross),
        "LS": bool(kd_overbought_cross),
        "M":  bool(wr_oversold),
        "MS": bool(wr_overbought),
        "N":  bool(ma_pull_bull),
        "NS": bool(ma_pull_bear),
        # 進階K棒型態（新增）
        "O":  bool(morning_star),
        "OS": bool(evening_star),
        "P":  bool(three_soldiers),
        "PS": bool(three_crows),
        "Q":  bool(inside_breakout),
        "QS": bool(inside_breakdown),
        # 均值回歸（新增）
        "R":  bool(bias_oversold),
        "RS": bool(bias_overbought),
    }


# ══════════════════════════════════════════════
# 向量化訊號預計算（加速回測核心）
# ══════════════════════════════════════════════
def _precompute_signals_vec(sub: pd.DataFrame,
                             vol_mult: float, min_avg_vol: int) -> np.ndarray:
    """
    一次性向量化計算所有 18 個策略在每一根 K 棒的訊號。
    等同於對每個 i 呼叫 _check(sub, i, ...)，但速度快 100~1000 倍。

    回傳 numpy bool 陣列，shape = (n_rows, 18)，
    欄位順序與 _STRAT_KEYS / STRATEGY_NAMES 一致。
    """
    c = sub["Close"].astype(float).values
    o = sub["Open"].astype(float).values
    h = sub["High"].astype(float).values
    l = sub["Low"].astype(float).values
    v = sub["Vol_K"].astype(float).values
    n = len(sub)

    # pandas Series（用於 rolling）
    c_s = pd.Series(c)
    h_s = pd.Series(h)
    l_s = pd.Series(l)
    v_s = pd.Series(v)

    # ── Moving averages ──────────────────────────────────────────
    ma5_v  = c_s.rolling(5).mean().values
    ma10_v = c_s.rolling(10).mean().values
    ma20_v = c_s.rolling(20).mean().values

    # ── Volume averages（前 N 日均量，不含今日）────────────────
    avg5_v  = v_s.shift(1).rolling(5).mean().values
    avg20_v = v_s.shift(1).rolling(20).mean().values

    # ── Base filter：需 ≥20 根歷史 + 5日均量達標 ───────────────
    base_ok = np.zeros(n, dtype=bool)
    if n > 20:
        base_ok[20:] = True
    with np.errstate(invalid="ignore"):
        vol_ok = base_ok & (avg5_v >= min_avg_vol)

    # ── Vol ratio ─────────────────────────────────────────────────
    with np.errstate(divide="ignore", invalid="ignore"):
        vol_ratio = np.where(avg5_v > 0, v / avg5_v, np.nan)

    # ── Price change & gap ───────────────────────────────────────
    prev_close = np.empty(n); prev_close[0] = np.nan; prev_close[1:] = c[:-1]
    with np.errstate(divide="ignore", invalid="ignore"):
        chg_pct = (c - prev_close) / prev_close * 100
        gap     = (o - prev_close) / prev_close * 100

    # ── RSI (14-period) ──────────────────────────────────────────
    rsi_s    = _rsi(c_s, 14)
    rsi_v    = rsi_s.values
    rsi_prev = np.empty(n); rsi_prev[0] = np.nan; rsi_prev[1:] = rsi_v[:-1]

    # ── 5-day high/low（前 5 日，不含今日）─────────────────────
    hi5_v = h_s.shift(1).rolling(5).max().values
    lo5_v = l_s.shift(1).rolling(5).min().values

    # ── Three consecutive candles（前 3 日）─────────────────────
    bull_c = (c > o)
    bear_c = (c < o)
    # shift(k) → 前 k 根
    def _shift(arr, k):
        out = np.empty(n, dtype=bool); out[:k] = False; out[k:] = arr[:-k]; return out
    three_up = _shift(bull_c, 1) & _shift(bull_c, 2) & _shift(bull_c, 3)
    three_dn = _shift(bear_c, 1) & _shift(bear_c, 2) & _shift(bear_c, 3)

    # ── Volume expansion / contraction ───────────────────────────
    with np.errstate(invalid="ignore"):
        vol_expand = (avg5_v > avg20_v * 1.2)
        vol_shrink = (avg5_v < avg20_v * 0.8)

    # ── Tide shrink（連續 3 日縮量）────────────────────────────
    def _shiftf(arr, k):
        out = np.empty(n); out[:k] = np.nan; out[k:] = arr[:-k]; return out
    tide_shrink = (
        (_shiftf(v, 1) < _shiftf(v, 2)) &
        (_shiftf(v, 2) < _shiftf(v, 3)) &
        (_shiftf(v, 3) < _shiftf(v, 4))
    )

    # ── K-bar patterns ───────────────────────────────────────────
    body      = np.abs(c - o)
    upper     = h - np.maximum(c, o)
    lower     = np.minimum(c, o) - l
    prev_body = _shiftf(body, 1)
    prev_bull = _shift(bull_c, 1)
    prev_bear = _shift(bear_c, 1)
    prev_c    = _shiftf(c, 1)
    prev_o    = _shiftf(o, 1)

    with np.errstate(invalid="ignore"):
        hammer = (
            (body > 0) & (lower >= 2 * body) & (upper <= body) &
            (chg_pct < 0) & prev_bear
        )
        shoot_star = (
            (body > 0) & (upper >= 2 * body) & (lower <= body) &
            (chg_pct > 0) & prev_bull
        )
        engulf_bull = (
            bull_c & prev_bear &
            (o <= prev_c) & (c >= prev_o) &
            (body > prev_body * 0.8)
        )
        engulf_bear = (
            bear_c & prev_bull &
            (o >= prev_c) & (c <= prev_o) &
            (body > prev_body * 0.8)
        )

    # ── 技術指標類（新增）─────────────────────────────────
    # J/JS: MACD 黃金/死亡交叉 (12/26/9)
    ema12_v    = c_s.ewm(span=12, adjust=False).mean().values
    ema26_v    = c_s.ewm(span=26, adjust=False).mean().values
    macd_v     = ema12_v - ema26_v
    macd_sig_v = pd.Series(macd_v).ewm(span=9, adjust=False).mean().values
    macd_prev  = _shiftf(macd_v,     1)
    msig_prev  = _shiftf(macd_sig_v, 1)
    with np.errstate(invalid="ignore"):
        macd_golden_v = (macd_prev < msig_prev) & (macd_v > macd_sig_v)
        macd_death_v  = (macd_prev > msig_prev) & (macd_v < macd_sig_v)

    # K/KS: 布林通道下軌反彈 / 上軌反壓 (20, 2)
    bb_std_v    = c_s.rolling(20).std().values
    bb_upper_v  = ma20_v + 2 * bb_std_v
    bb_lower_v  = ma20_v - 2 * bb_std_v
    prev_bb_lo  = _shiftf(bb_lower_v, 1)
    prev_bb_up  = _shiftf(bb_upper_v, 1)
    with np.errstate(invalid="ignore"):
        bb_bounce_v    = (prev_c <= prev_bb_lo) & (c > bb_lower_v)
        bb_rejection_v = (prev_c >= prev_bb_up) & (c < bb_upper_v)

    # L/LS: KD 超賣黃金交叉(<30) / 超買死亡交叉(>70)
    hi9_v  = h_s.rolling(9).max().values
    lo9_v  = l_s.rolling(9).min().values
    with np.errstate(divide="ignore", invalid="ignore"):
        rsv_v  = np.where((hi9_v - lo9_v) > 0,
                          (c - lo9_v) / (hi9_v - lo9_v) * 100, 50.0)
    kd_k_v  = pd.Series(rsv_v).ewm(com=2, adjust=False).mean().values
    kd_d_v  = pd.Series(kd_k_v).ewm(com=2, adjust=False).mean().values
    kd_kp   = _shiftf(kd_k_v, 1)
    kd_dp   = _shiftf(kd_d_v, 1)
    with np.errstate(invalid="ignore"):
        kd_oversold_v   = (kd_k_v < 30) & (kd_kp < kd_dp) & (kd_k_v > kd_d_v)
        kd_overbought_v = (kd_k_v > 70) & (kd_kp > kd_dp) & (kd_k_v < kd_d_v)

    # M/MS: 威廉%R 超賣(<-80)反彈 / 超買(>-20)回落
    hi14_v = h_s.rolling(14).max().values
    lo14_v = l_s.rolling(14).min().values
    with np.errstate(divide="ignore", invalid="ignore"):
        wr_v = np.where((hi14_v - lo14_v) > 0,
                        (hi14_v - c) / (hi14_v - lo14_v) * -100, -50.0)
    wr_prev_v = _shiftf(wr_v, 1)
    with np.errstate(invalid="ignore"):
        wr_oversold_v   = (wr_prev_v < -80) & (wr_v > wr_prev_v) & (chg_pct > 0)
        wr_overbought_v = (wr_prev_v > -20) & (wr_v < wr_prev_v) & (chg_pct < 0)

    # N/NS: 均線多頭排列+昨收跌破ma5今收站回 / 空頭排列+昨收突破ma5今收跌破
    ma_bull_v    = (ma5_v > ma10_v) & (ma10_v > ma20_v)
    ma_bear_v    = (ma5_v < ma10_v) & (ma10_v < ma20_v)
    with np.errstate(invalid="ignore"):
        ma_pull_bull_v = ma_bull_v & (prev_c < ma5_v) & (c >= ma5_v)
        ma_pull_bear_v = ma_bear_v & (prev_c > ma5_v) & (c <= ma5_v)

    # ── 進階K棒型態（新增）─────────────────────────────────
    # O/OS: 晨星 / 黃昏之星（三K棒，需至少3根）
    d1_c_v    = _shiftf(c, 2);   d1_o_v    = _shiftf(o, 2)
    d1_body_v = np.abs(d1_c_v - d1_o_v)
    d2_body_v = _shiftf(body, 1)
    d1_mid_v  = (d1_c_v + d1_o_v) / 2
    with np.errstate(invalid="ignore"):
        morning_star_v = (
            (d1_c_v < d1_o_v) &                   # Day1 陰線
            (d1_body_v > 0) &                      # Day1 有實體
            (d2_body_v < d1_body_v * 0.4) &        # Day2 小實體
            bull_c &                               # Day3 陽線
            (c > d1_mid_v) &                       # Day3 收超過Day1中點
            (body >= d1_body_v * 0.5)              # Day3 實體夠大
        )
        evening_star_v = (
            (d1_c_v > d1_o_v) &                   # Day1 陽線
            (d1_body_v > 0) &
            (d2_body_v < d1_body_v * 0.4) &
            bear_c &                               # Day3 陰線
            (c < d1_mid_v) &                       # Day3 收低於Day1中點
            (body >= d1_body_v * 0.5)
        )

    # P/PS: 紅三兵 / 黑三兵
    d1_bull_v  = _shift(bull_c, 2); d2_bull_v = _shift(bull_c, 1)
    d1_bear_v  = _shift(bear_c, 2); d2_bear_v = _shift(bear_c, 1)
    d1_c2_v    = _shiftf(c, 2);     d2_c2_v   = _shiftf(c, 1)
    d1_o2_v    = _shiftf(o, 2);     d2_o2_v   = _shiftf(o, 1)
    with np.errstate(invalid="ignore"):
        three_soldiers_v = (
            d1_bull_v & d2_bull_v & bull_c &
            (d2_c2_v > d1_c2_v) & (c > d2_c2_v) &
            (d2_o2_v >= d1_o2_v) & (o >= d2_o2_v)
        )
        three_crows_v = (
            d1_bear_v & d2_bear_v & bear_c &
            (d2_c2_v < d1_c2_v) & (c < d2_c2_v) &
            (d2_o2_v <= d1_o2_v) & (o <= d2_o2_v)
        )

    # Q/QS: Inside Bar 突破/跌破
    prev2_h_v  = _shiftf(h, 2); prev2_l_v = _shiftf(l, 2)
    prev1_h_v  = _shiftf(h, 1); prev1_l_v = _shiftf(l, 1)
    is_inside_v = (prev1_h_v < prev2_h_v) & (prev1_l_v > prev2_l_v)
    with np.errstate(invalid="ignore"):
        inside_breakout_v  = is_inside_v & (c > prev2_h_v) & (vol_ratio >= vol_mult)
        inside_breakdown_v = is_inside_v & (c < prev2_l_v) & (vol_ratio >= vol_mult)

    # ── 均值回歸（新增）────────────────────────────────────
    # R/RS: BIAS 乖離率（對 MA20）
    with np.errstate(divide="ignore", invalid="ignore"):
        bias_v = np.where(ma20_v > 0, (c - ma20_v) / ma20_v * 100, 0.0)
    bias_oversold_v   = (bias_v < -8) & (chg_pct > 0)
    bias_overbought_v = (bias_v > +8) & (chg_pct < 0)

    # ── 組合成訊號陣列（shape: n × 36，欄位順序 = _STRAT_KEYS）──
    def _f(cond):
        """NaN-safe bool：NaN → False"""
        return np.where(np.isnan(cond.astype(float)), False, cond).astype(bool) & vol_ok

    with np.errstate(invalid="ignore"):
        sig = np.column_stack([
            _f((ma5_v > ma20_v)   & (vol_ratio >= vol_mult)  & (chg_pct > 0)),   # A
            _f((ma5_v < ma20_v)   & (vol_ratio >= vol_mult)  & (chg_pct < 0)),   # AS
            _f((gap >= 2.0)        & (vol_ratio >= 1.3)       & (chg_pct > 0)),   # B
            _f((gap <= -2.0)       & (vol_ratio >= 1.3)       & (chg_pct < 0)),   # BS
            _f((rsi_prev < 35)     & (rsi_v > rsi_prev)       & (c > ma5_v)),     # C
            _f((rsi_prev > 65)     & (rsi_v < rsi_prev)       & (c < ma5_v)),     # CS
            _f((c > hi5_v)         & (vol_ratio >= vol_mult)),                     # D
            _f((c < lo5_v)         & (vol_ratio >= vol_mult)),                     # DS
            _f(three_up            & (ma5_v > ma10_v) & (ma10_v > ma20_v) & (chg_pct > 0)),  # E
            _f(three_dn            & (ma5_v < ma10_v) & (ma10_v < ma20_v) & (chg_pct < 0)),  # ES
            _f(vol_expand          & (vol_ratio >= vol_mult)  & (chg_pct > 0)),   # F
            _f(vol_shrink          & (vol_ratio >= vol_mult)  & (chg_pct < 0)),   # FS
            _f(tide_shrink         & (vol_ratio >= vol_mult)  & (chg_pct > 0)),   # G
            _f(tide_shrink         & (vol_ratio >= vol_mult)  & (chg_pct < 0)),   # GS
            _f(hammer),                                                            # H
            _f(shoot_star),                                                        # HS
            _f(engulf_bull),                                                       # I
            _f(engulf_bear),                                                       # IS
            _f(macd_golden_v       & (chg_pct > 0)),                              # J
            _f(macd_death_v        & (chg_pct < 0)),                              # JS
            _f(bb_bounce_v),                                                       # K
            _f(bb_rejection_v),                                                    # KS
            _f(kd_oversold_v),                                                     # L
            _f(kd_overbought_v),                                                   # LS
            _f(wr_oversold_v),                                                     # M
            _f(wr_overbought_v),                                                   # MS
            _f(ma_pull_bull_v),                                                    # N
            _f(ma_pull_bear_v),                                                    # NS
            _f(morning_star_v),                                                    # O
            _f(evening_star_v),                                                    # OS
            _f(three_soldiers_v),                                                  # P
            _f(three_crows_v),                                                     # PS
            _f(inside_breakout_v),                                                 # Q
            _f(inside_breakdown_v),                                                # QS
            _f(bias_oversold_v),                                                   # R
            _f(bias_overbought_v),                                                 # RS
        ])  # shape (n, 36), dtype bool

    return sig


# ══════════════════════════════════════════════
# 選股
# ══════════════════════════════════════════════
def run_scan(data: dict, min_hit: int, vol_mult: float,
             min_avg_vol: int, strategy_filter: list = None,
             show_combos: list = None, workers: int = 4) -> list:
    """
    對每檔股票計算今日訊號，回傳通過命中門檻的候選清單。
    使用 ThreadPoolExecutor 平行處理各股票以加速掃描。

    show_combos : 若指定（如 ['C+F+I','B+E+F']），只保留至少命中其中一個組合的股票，
                  並在結果中附上「命中組合」欄位。
    """
    # 預先解析每個組合的策略集合
    combo_specs = None
    if show_combos:
        combo_specs = [(c, c.split("+")) for c in show_combos]

    def _process_stock(item):
        code, df = item
        if len(df) < 22:
            return None

        sigs = _check(df, len(df) - 1, min_avg_vol, vol_mult)

        # ── 組合過濾 ─────────────────────────────────────────────
        matched_combos = []
        if combo_specs is not None:
            for combo_key, strats in combo_specs:
                if all(sigs.get(s, False) for s in strats):
                    matched_combos.append(combo_key)
            if not matched_combos:
                return None

        # ── 命中數計算 ───────────────────────────────────────────
        if strategy_filter:
            long_hit  = sum(1 for k, v in sigs.items() if v and k in strategy_filter and not k.endswith("S"))
            short_hit = sum(1 for k, v in sigs.items() if v and k in strategy_filter and k.endswith("S"))
        else:
            long_hit  = sum(1 for k, v in sigs.items() if v and not k.endswith("S"))
            short_hit = sum(1 for k, v in sigs.items() if v and k.endswith("S"))
        hit = max(long_hit, short_hit)

        if combo_specs is None and hit < min_hit:
            return None

        direction = "多" if long_hit >= short_hit else "空"
        today     = df.iloc[-1]
        prev      = df.iloc[-2] if len(df) >= 2 else today
        close     = round(float(today["Close"]), 2)
        prev_cl   = float(prev["Close"])
        chg       = round((close - prev_cl) / prev_cl * 100, 2) if prev_cl else 0
        vol_k     = int(round(float(today["Vol_K"])))
        avg5      = float(df["Vol_K"].iloc[-6:-1].mean())
        vol_r     = f"{vol_k/avg5:.1f}x" if avg5 > 0 else "—"
        strats    = ",".join(k for k, v in sigs.items() if v)
        try:
            data_date = str(df.index[-1].date())
        except Exception:
            data_date = "—"

        return {
            "代號":      code,
            "名稱":      get_name(code),
            "資料日期":  data_date,
            "收盤":      close,
            "漲跌幅(%)": chg,
            "成交量(張)": vol_k,
            "量/均量":   vol_r,
            "A多":  "✅" if sigs["A"]  else "❌",
            "AS空": "✅" if sigs["AS"] else "❌",
            "B多":  "✅" if sigs["B"]  else "❌",
            "BS空": "✅" if sigs["BS"] else "❌",
            "C多":  "✅" if sigs["C"]  else "❌",
            "CS空": "✅" if sigs["CS"] else "❌",
            "D多":  "✅" if sigs["D"]  else "❌",
            "DS空": "✅" if sigs["DS"] else "❌",
            "E多":  "✅" if sigs["E"]  else "❌",
            "ES空": "✅" if sigs["ES"] else "❌",
            "F均量多":  "✅" if sigs["F"]  else "❌",
            "FS均量空": "✅" if sigs["FS"] else "❌",
            "G潮汐多":  "✅" if sigs["G"]  else "❌",
            "GS潮汐空": "✅" if sigs["GS"] else "❌",
            "H鎚子":    "✅" if sigs["H"]  else "❌",
            "HS射擊":   "✅" if sigs["HS"] else "❌",
            "I吞多":    "✅" if sigs["I"]  else "❌",
            "IS吞空":   "✅" if sigs["IS"] else "❌",
            "命中數":    hit,
            "方向":      direction,
            "策略清單":  strats,
            "命中組合":  " | ".join(matched_combos) if matched_combos else "—",
        }

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = [r for r in pool.map(_process_stock, data.items()) if r is not None]

    results.sort(key=lambda x: (-x["命中數"], -x["漲跌幅(%)"]))
    return results


# ══════════════════════════════════════════════
# 回測
# ══════════════════════════════════════════════
def run_backtest(data: dict, days: int, min_hit: int,
                 vol_mult: float, min_avg_vol: int,
                 stop_loss: float,
                 take_profit: float = 0.0,
                 exit_day: int = 2,
                 short_only: bool = False,
                 long_only: bool = False,
                 strategy_filter: list = None,
                 workers: int = 4) -> list:
    """
    對所有股票跑 days 天歷史回測。
    使用向量化訊號預計算 + ThreadPoolExecutor 平行處理以大幅加速。

    進出場邏輯（exit_day 控制）：
      D0 收盤訊號確認 → D1 開盤進場
      exit_day=1：D1 收盤出場（隔日當沖）
      exit_day=2：D2 開盤出場（持有一日，預設）
      exit_day=3：D3 開盤出場（持有兩日）

    stop_loss  > 0：D1~D{exit_day} 任一天觸及停損線即出場（估算）
    take_profit > 0：D1~D{exit_day} 任一天觸及止盈線即出場（估算）
    short_only：True 時只統計空方策略
    """

    def _process_stock(item):
        code, df = item
        buf     = days + 35
        df_sl   = df.iloc[-min(buf, len(df)):]
        n       = len(df_sl)
        start_i = max(20, n - days)
        if n < 22 or start_i >= n - exit_day:
            return {k: [] for k in STRATEGY_NAMES}

        # 保留原始日期
        try:
            row_dates = [str(idx.date()) for idx in df_sl.index]
        except Exception:
            row_dates = [str(j) for j in range(n)]

        sub     = df_sl.reset_index(drop=True)
        sig_arr = _precompute_signals_vec(sub, vol_mult, min_avg_vol)  # (n, 18) bool

        local = {k: [] for k in STRATEGY_NAMES}

        for i in range(start_i, n - exit_day):
            row = sig_arr[i]

            long_hits  = int(sum(row[j] for j in range(_N_STRATS) if not _IS_SHORT[j]))
            short_hits = int(sum(row[j] for j in range(_N_STRATS) if _IS_SHORT[j]))

            if max(long_hits, short_hits) < min_hit:
                continue

            d1_open = float(sub.iloc[i + 1]["Open"])
            if d1_open == 0:
                continue

            # 出場價
            if exit_day == 1:
                exit_price = float(sub.iloc[i + 1]["Close"])
            else:
                if i + exit_day >= n:
                    continue
                exit_price = float(sub.iloc[i + exit_day]["Open"])
                if exit_price == 0:
                    continue

            hold_high = max(float(sub.iloc[i + j]["High"]) for j in range(1, exit_day + 1))
            hold_low  = min(float(sub.iloc[i + j]["Low"])  for j in range(1, exit_day + 1))

            date_str = row_dates[i + 1] if i + 1 < len(row_dates) else str(i + 1)

            for ji, k in enumerate(_STRAT_KEYS):
                if not row[ji]:
                    continue
                is_short = _IS_SHORT[ji]
                if short_only and not is_short:
                    continue
                if long_only and is_short:
                    continue
                if strategy_filter and k not in strategy_filter:
                    continue
                hit_count = short_hits if is_short else long_hits
                if hit_count < min_hit:
                    continue

                stopped = False
                if stop_loss > 0:
                    if is_short:
                        stopped = hold_high >= d1_open * (1 + stop_loss / 100)
                    else:
                        stopped = hold_low  <= d1_open * (1 - stop_loss / 100)

                taken = False
                if not stopped and take_profit > 0:
                    if is_short:
                        taken = hold_low  <= d1_open * (1 - take_profit / 100)
                    else:
                        taken = hold_high >= d1_open * (1 + take_profit / 100)

                if stopped:
                    ret_net = -(stop_loss   + TRADE_COST * 100)
                elif taken:
                    ret_net =  take_profit  - TRADE_COST * 100
                elif is_short:
                    ret_net = (d1_open - exit_price) / d1_open * 100 - TRADE_COST * 100
                else:
                    ret_net = (exit_price - d1_open) / d1_open * 100 - TRADE_COST * 100

                local[k].append({
                    "code":    code,
                    "date":    date_str,
                    "ret_net": round(ret_net, 3),
                    "win":     ret_net > 0,
                    "stopped": stopped,
                    "taken":   taken,
                })

        return local

    # ── 平行處理各股票 ────────────────────────────────────────
    trades = {k: [] for k in STRATEGY_NAMES}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for loc in pool.map(_process_stock, data.items()):
            for k in STRATEGY_NAMES:
                trades[k].extend(loc.get(k, []))

    # ── 彙整統計 ──────────────────────────────────────────────
    rows = []
    for k, name in STRATEGY_NAMES.items():
        t = trades[k]
        if not t:
            rows.append({
                "策略": f"{k}｜{name}", "訊號次數": 0,
                "勝率(%)": "-", "平均獲利(%)": "-",
                "平均虧損(%)": "-", "期望值(%)": "-",
                "最大單筆虧損(%)": "-", "停損觸發(%)": "-", "止盈觸發(%)": "-",
            })
            continue
        wins    = [x["ret_net"] for x in t if x["win"]]
        losses  = [x["ret_net"] for x in t if not x["win"]]
        stopped = sum(1 for x in t if x["stopped"])
        taken   = sum(1 for x in t if x.get("taken", False))
        wr      = len(wins) / len(t) * 100
        avg_w   = sum(wins)   / len(wins)   if wins   else 0
        avg_l   = sum(losses) / len(losses) if losses else 0
        ev      = wr/100 * avg_w + (1-wr/100) * avg_l
        max_dd  = min((x["ret_net"] for x in t), default=0)
        stp_pct = round(stopped / len(t) * 100, 1) if stop_loss   > 0 else "-"
        tkp_pct = round(taken   / len(t) * 100, 1) if take_profit > 0 else "-"
        rows.append({
            "策略":              f"{k}｜{name}",
            "訊號次數":          len(t),
            "勝率(%)":           round(wr,     1),
            "平均獲利(%)":       round(avg_w,  3),
            "平均虧損(%)":       round(avg_l,  3),
            "期望值(%)":         round(ev,     3),
            "最大單筆虧損(%)":   round(max_dd, 3),
            "停損觸發(%)":       stp_pct,
            "止盈觸發(%)":       tkp_pct,
        })
    return rows


# ══════════════════════════════════════════════
# 策略組合分析
# ══════════════════════════════════════════════
def run_combo_analysis(data: dict, days: int, min_hit: int,
                       vol_mult: float, min_avg_vol: int,
                       stop_loss: float = 0.0,
                       direction: str = "long",
                       min_signals: int = 10,
                       max_combo: int = 3,
                       workers: int = 4) -> list:
    """
    計算所有 2 到 max_combo 策略組合的勝率與期望值。
    每個組合的每筆交易只計算一次，正確反映「同時符合所有條件才進場」的績效。
    使用向量化訊號預計算 + ThreadPoolExecutor 平行處理以大幅加速。

    direction : "long"（多方組合）或 "short"（空方組合）
    min_signals: 低於此訊號數的組合標記為 ⚠️ 樣本不足
    max_combo  : 最大組合策略數（預設 3；太大時組合數爆炸且樣本極少）
    """
    from itertools import combinations as _comb

    is_short_dir = (direction == "short")
    candidates   = [k for k in _STRAT_KEYS
                    if (k.endswith("S") if is_short_dir else not k.endswith("S"))]
    # 候選策略在 _STRAT_KEYS 中的位置索引
    cand_idx     = [_STRAT_KEYS.index(k) for k in candidates]

    def _process_stock(item):
        code, df = item
        buf     = days + 35
        df_sl   = df.iloc[-min(buf, len(df)):]
        n       = len(df_sl)
        start_i = max(20, n - days)
        if n < 22 or start_i >= n - 1:
            return {}

        sub     = df_sl.reset_index(drop=True)
        sig_arr = _precompute_signals_vec(sub, vol_mult, min_avg_vol)  # (n, 18) bool

        local_combo: dict = {}  # key → list[float]

        for i in range(start_i, n - 1):
            row = sig_arr[i]
            hit = [candidates[j] for j, cidx in enumerate(cand_idx) if row[cidx]]
            if len(hit) < 2:
                continue

            d0_close = float(sub.iloc[i]["Close"])
            d1       = sub.iloc[i + 1]
            d1_open  = float(d1["Open"])
            d1_high  = float(d1["High"])
            d1_low   = float(d1["Low"])
            d1_close = float(d1["Close"])
            if d1_open == 0 or d0_close == 0:
                continue

            lim_up   = calc_limit_up(d0_close)
            lim_down = calc_limit_down(d0_close)

            # ── 出場計算（只算一次，供所有組合共用）────
            stopped = False
            if stop_loss > 0:
                if is_short_dir:
                    stopped = d1_high >= d1_open * (1 + stop_loss / 100)
                else:
                    stopped = d1_low  <= d1_open * (1 - stop_loss / 100)

            if stopped:
                ret_net = -(stop_loss + TRADE_COST * 100)
            elif not is_short_dir:
                exit_p  = lim_up   if d1_high >= lim_up   else d1_close
                ret_net = (exit_p - d1_open) / d1_open * 100 - TRADE_COST * 100
            else:
                exit_p  = lim_down if d1_low  <= lim_down else d1_close
                ret_net = (d1_open - exit_p) / d1_open * 100 - TRADE_COST * 100

            ret_net = round(ret_net, 3)

            cap = min(max_combo, len(hit))
            for size in range(2, cap + 1):
                for combo in _comb(hit, size):
                    key = "+".join(combo)
                    if key not in local_combo:
                        local_combo[key] = []
                    local_combo[key].append(ret_net)

        return local_combo

    # ── 平行處理各股票，主執行緒合併結果 ─────────────────────
    combo_trades: dict = {}
    total = len(data)
    done  = 0
    print(f"  🔄 策略組合分析：處理 {total} 檔股票（{max_combo} 策略最多同時命中）...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for local_combo in pool.map(_process_stock, data.items()):
            for key, rets in local_combo.items():
                if key not in combo_trades:
                    combo_trades[key] = []
                combo_trades[key].extend(rets)
            done += 1
            if done % 50 == 0 or done == total:
                print(f"     進度：{done}/{total} 檔  組合數：{len(combo_trades)}", end="\r", flush=True)
    print()  # 換行

    # ── 彙整統計 ──────────────────────────────────
    rows = []
    for key, rets in combo_trades.items():
        n_sig  = len(rets)
        wins   = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        wr     = len(wins) / n_sig * 100
        avg_w  = sum(wins)   / len(wins)   if wins   else 0.0
        avg_l  = sum(losses) / len(losses) if losses else 0.0
        ev     = wr / 100 * avg_w + (1 - wr / 100) * avg_l
        max_dd = min(rets)
        rows.append({
            "組合":          key,
            "訊號次數":      n_sig,
            "勝率(%)":       round(wr,    1),
            "平均獲利(%)":   round(avg_w, 3),
            "平均虧損(%)":   round(avg_l, 3),
            "期望值(%)":     round(ev,    3),
            "最大單筆虧損(%)": round(max_dd, 3),
            "樣本":          "⚠️少" if n_sig < min_signals else "OK",
        })

    rows.sort(key=lambda x: (-x["勝率(%)"], -x["期望值(%)"]))
    return rows


def print_combo_result(rows: list, days: int, direction: str,
                       stop_loss: float, min_signals: int = 10,
                       max_combo: int = 3,
                       show_combos: list = None):
    """
    show_combos : 若指定（如 ['C+F+I','B+E+F','B+E','B+D+E']），只顯示這幾個組合。
                  None = 全部顯示。
    """
    dir_str  = "多方" if direction == "long" else "空方"
    sl_str   = f"-{stop_loss}%（估算）" if stop_loss > 0 else "未設定"
    size_str = f"2 ~ {max_combo} 策略同時命中"
    print("\n" + "═"*85)
    print(f"  🔗 策略組合分析（{dir_str}，{size_str}）  回測天數：{days} 日  停損：{sl_str}")
    print(f"  邏輯：組合內所有策略同時觸發才進場，每筆交易只計算一次（無重複計入）")
    if show_combos:
        print(f"  📌 僅顯示指定組合：{', '.join(show_combos)}")
    print(f"  ⚠️少 = 樣本數 < {min_signals} 筆，統計上不可靠，供參考")
    print("═"*85)

    if not rows:
        print("  （無符合條件的策略組合）")
        print("═"*85 + "\n")
        return

    # 若有 show_combos，依指定清單篩選並保持指定順序
    display_rows = rows
    if show_combos:
        want = [c.strip() for c in show_combos]
        row_map = {r["組合"]: r for r in rows}
        display_rows = []
        for key in want:
            if key in row_map:
                display_rows.append(row_map[key])
            else:
                print(f"  ⚠️  找不到組合 '{key}'（回測結果中不存在，可能樣本為 0）")
        if not display_rows:
            print("  （指定組合均無資料）")
            print("═"*85 + "\n")
            return
    else:
        # 無指定組合時：分別印出「期望值 Top 30」與「勝率 Top 30」
        reliable = [r for r in rows if r["樣本"] == "OK"]
        total_combos = len(rows)

        print(f"  共計算 {total_combos} 個組合"
              f"（樣本充足 {len(reliable)} 個，⚠️少 {total_combos - len(reliable)} 個）")
        print()

        # ── 期望值 Top 30（樣本充足優先）──────────────────
        top_ev = sorted(reliable, key=lambda x: -x["期望值(%)"])[:30]
        if not top_ev:
            top_ev = sorted(rows, key=lambda x: -x["期望值(%)"])[:30]
        print(f"  📊 期望值 Top {len(top_ev)}（樣本充足，依期望值排序）")
        df_ev = pd.DataFrame(top_ev)
        cols  = ["組合","訊號次數","勝率(%)","平均獲利(%)","平均虧損(%)",
                 "期望值(%)","最大單筆虧損(%)","樣本"]
        print_table(df_ev, cols)

        # ── 勝率 Top 20（樣本充足優先）──────────────────
        top_wr = sorted(reliable, key=lambda x: (-x["勝率(%)"], -x["期望值(%)"]))[:20]
        if top_wr and top_wr[0]["組合"] != top_ev[0]["組合"]:
            print(f"\n  🏆 勝率 Top {len(top_wr)}（樣本充足，依勝率排序）")
            df_wr = pd.DataFrame(top_wr)
            print_table(df_wr, cols)

        print("═"*85 + "\n")
        if reliable:
            best_ev_r = top_ev[0] if top_ev else None
            best_wr_r = top_wr[0] if top_wr else None
            if best_ev_r:
                print(f"  💰 期望值最高：{best_ev_r['組合']}"
                      f"  EV {best_ev_r['期望值(%)']:+.3f}%  勝率 {best_ev_r['勝率(%)']}%"
                      f"  n={best_ev_r['訊號次數']}")
            if best_wr_r and best_wr_r['組合'] != (best_ev_r['組合'] if best_ev_r else ""):
                print(f"  🏆 勝率最高：{best_wr_r['組合']}"
                      f"  勝率 {best_wr_r['勝率(%)']}%  EV {best_wr_r['期望值(%)']:+.3f}%"
                      f"  n={best_wr_r['訊號次數']}")
        print("═"*85 + "\n")
        return

    df = pd.DataFrame(display_rows)
    cols = ["組合","訊號次數","勝率(%)","平均獲利(%)","平均虧損(%)",
            "期望值(%)","最大單筆虧損(%)","樣本"]
    print_table(df, cols)

    reliable = [r for r in display_rows if r["樣本"] == "OK"]
    if reliable:
        best_wr = max(reliable, key=lambda x: x["勝率(%)"])
        best_ev = max(reliable, key=lambda x: x["期望值(%)"])
        print(f"\n  🏆 勝率最高（樣本充足）：{best_wr['組合']}"
              f"  勝率 {best_wr['勝率(%)']}%  期望值 {best_wr['期望值(%)']:+.3f}%"
              f"  訊號 {best_wr['訊號次數']} 次")
        if best_ev["組合"] != best_wr["組合"]:
            print(f"  💰 期望值最高（樣本充足）：{best_ev['組合']}"
                  f"  期望值 {best_ev['期望值(%)']:+.3f}%  勝率 {best_ev['勝率(%)']}%"
                  f"  訊號 {best_ev['訊號次數']} 次")
    print("═"*85 + "\n")


# ══════════════════════════════════════════════
# 漲跌停出場回測
# ══════════════════════════════════════════════
def run_backtest_limit(data: dict, days: int, min_hit: int,
                       vol_mult: float, min_avg_vol: int,
                       stop_loss: float = 0.0,
                       strategy_filter: list = None,
                       return_trades: bool = False,
                       workers: int = 4):
    """
    漲跌停出場回測（隔日沖模式）。
    使用向量化訊號預計算 + ThreadPoolExecutor 平行處理以大幅加速。

    進出場邏輯：
      D0 收盤訊號確認 → D1 開盤進場
      多方：D1 盤中 High ≥ 漲停價 → 漲停價出場；否則 → D1 收盤出場
      空方：D1 盤中 Low  ≤ 跌停價 → 跌停價出場；否則 → D1 收盤出場

    stop_loss > 0（可選）：
      停損判斷優先於漲跌停；以 D1 High/Low 估算是否觸及停損。

    漲停/跌停：依台股最小升降單位精確計算（≈ ±10%，非剛好 10%）。
    """

    def _process_stock(item):
        code, df = item
        buf     = days + 35
        df_sl   = df.iloc[-min(buf, len(df)):]
        n       = len(df_sl)
        start_i = max(20, n - days)
        if n < 22 or start_i >= n - 1:
            return {k: [] for k in STRATEGY_NAMES}

        # 保留原始日期
        try:
            row_dates = [str(idx.date()) for idx in df_sl.index]
        except Exception:
            row_dates = [str(j) for j in range(n)]

        sub     = df_sl.reset_index(drop=True)
        sig_arr = _precompute_signals_vec(sub, vol_mult, min_avg_vol)  # (n, 18) bool

        local = {k: [] for k in STRATEGY_NAMES}

        for i in range(start_i, n - 1):
            row = sig_arr[i]

            long_hits  = int(sum(row[j] for j in range(_N_STRATS) if not _IS_SHORT[j]))
            short_hits = int(sum(row[j] for j in range(_N_STRATS) if _IS_SHORT[j]))

            if max(long_hits, short_hits) < min_hit:
                continue

            d0_close = float(sub.iloc[i]["Close"])
            d1       = sub.iloc[i + 1]
            d1_open  = float(d1["Open"])
            d1_high  = float(d1["High"])
            d1_low   = float(d1["Low"])
            d1_close = float(d1["Close"])

            if d1_open == 0 or d0_close == 0:
                continue

            lim_up   = calc_limit_up(d0_close)
            lim_down = calc_limit_down(d0_close)
            date_str = row_dates[i + 1] if i + 1 < len(row_dates) else str(i + 1)

            for ji, k in enumerate(_STRAT_KEYS):
                if not row[ji]:
                    continue
                is_short = _IS_SHORT[ji]
                if strategy_filter and k not in strategy_filter:
                    continue
                hit_count = short_hits if is_short else long_hits
                if hit_count < min_hit:
                    continue

                # ── 停損判斷（優先） ─────────────────────────
                stopped = False
                if stop_loss > 0:
                    if is_short:
                        stopped = d1_high >= d1_open * (1 + stop_loss / 100)
                    else:
                        stopped = d1_low  <= d1_open * (1 - stop_loss / 100)

                # ── 出場計算 ──────────────────────────────────
                if stopped:
                    ret_net   = -(stop_loss + TRADE_COST * 100)
                    exit_type = "停損"
                    exit_p    = (d1_open * (1 - stop_loss / 100) if not is_short
                                 else d1_open * (1 + stop_loss / 100))
                elif not is_short:
                    if d1_high >= lim_up:
                        exit_p    = lim_up
                        exit_type = "漲停"
                    else:
                        exit_p    = d1_close
                        exit_type = "收盤"
                    ret_net = (exit_p - d1_open) / d1_open * 100 - TRADE_COST * 100
                else:
                    if d1_low <= lim_down:
                        exit_p    = lim_down
                        exit_type = "跌停"
                    else:
                        exit_p    = d1_close
                        exit_type = "收盤"
                    ret_net = (d1_open - exit_p) / d1_open * 100 - TRADE_COST * 100

                local[k].append({
                    "code":      code,
                    "name":      get_name(code),
                    "date":      date_str,
                    "d0_close":  round(d0_close, 2),
                    "lim_up":    round(lim_up,   2),
                    "lim_down":  round(lim_down, 2),
                    "d1_open":   round(d1_open,  2),
                    "d1_high":   round(d1_high,  2),
                    "d1_low":    round(d1_low,   2),
                    "d1_close":  round(d1_close, 2),
                    "exit_p":    round(exit_p,   2),
                    "exit_type": exit_type,
                    "ret_net":   round(ret_net, 3),
                    "win":       ret_net > 0,
                    "stopped":   stopped,
                    "lim_exit":  (not stopped) and exit_type in ("漲停", "跌停"),
                    "is_short":  is_short,
                    "strategy":  k,
                })

        return local

    # ── 平行處理各股票 ────────────────────────────────────────
    trades = {k: [] for k in STRATEGY_NAMES}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for loc in pool.map(_process_stock, data.items()):
            for k in STRATEGY_NAMES:
                trades[k].extend(loc.get(k, []))

    # ── 彙整統計 ──────────────────────────────────
    rows = []
    for k, name in STRATEGY_NAMES.items():
        t = trades[k]
        if not t:
            rows.append({
                "策略":          f"{k}｜{name}",
                "訊號次數":      0,
                "勝率(%)":       "-",
                "平均獲利(%)":   "-",
                "平均虧損(%)":   "-",
                "期望值(%)":     "-",
                "最大單筆虧損(%)": "-",
                "停損觸發(%)":   "-",
                "漲跌停出場(%)": "-",
            })
            continue

        wins    = [x["ret_net"] for x in t if x["win"]]
        losses  = [x["ret_net"] for x in t if not x["win"]]
        stopped = sum(1 for x in t if x["stopped"])
        lim_ex  = sum(1 for x in t if x.get("lim_exit", False))
        wr      = len(wins) / len(t) * 100
        avg_w   = sum(wins)   / len(wins)   if wins   else 0.0
        avg_l   = sum(losses) / len(losses) if losses else 0.0
        ev      = wr / 100 * avg_w + (1 - wr / 100) * avg_l
        max_dd  = min((x["ret_net"] for x in t), default=0)
        stp_pct = round(stopped / len(t) * 100, 1) if stop_loss > 0 else "-"
        lim_pct = round(lim_ex  / len(t) * 100, 1)

        rows.append({
            "策略":              f"{k}｜{name}",
            "訊號次數":          len(t),
            "勝率(%)":           round(wr,     1),
            "平均獲利(%)":       round(avg_w,  3),
            "平均虧損(%)":       round(avg_l,  3),
            "期望值(%)":         round(ev,     3),
            "最大單筆虧損(%)":   round(max_dd, 3),
            "停損觸發(%)":       stp_pct,
            "漲跌停出場(%)":     lim_pct,
        })

    if return_trades:
        # 回傳 (統計彙整, 明細交易清單)
        all_trades = []
        for k, t in trades.items():
            all_trades.extend(t)
        all_trades.sort(key=lambda x: x["date"])
        return rows, all_trades
    return rows


# ══════════════════════════════════════════════
# 顯示工具
# ══════════════════════════════════════════════
def _dw(s: str) -> int:
    w = 0
    for c in str(s):
        w += 2 if unicodedata.east_asian_width(c) in ('W','F','A') else 1
    return w

def _pad(s: str, width: int, align: str = "left") -> str:
    s   = str(s)
    pad = max(0, width - _dw(s))
    return (" " * pad + s) if align == "right" else (s + " " * pad)

RIGHT = {"收盤","漲跌幅(%)","成交量(張)","命中數","訊號次數",
         "勝率(%)","平均獲利(%)","平均虧損(%)","期望值(%)",
         "最大單筆虧損(%)","停損觸發(%)"}

def _wrap_header(text: str, width: int) -> list:
    """將標題文字切成每行最多 width 顯示寬度的多行清單。"""
    lines, cur, cur_w = [], "", 0
    for ch in text:
        ch_w = _dw(ch)
        if cur_w + ch_w > width:
            lines.append(cur)
            cur, cur_w = ch, ch_w
        else:
            cur += ch
            cur_w += ch_w
    if cur:
        lines.append(cur)
    return lines or [""]

def print_table(df: pd.DataFrame, cols: list, title: str = ""):
    if title:
        print(f"\n  {title}")
    widths = {}
    for col in cols:
        if col not in df.columns:
            continue
        # 欄寬只由「資料」決定，標題過長時自動換行 —— 最小 3 避免太窄
        data_w = max((_dw(str(v)) for v in df[col]), default=0)
        widths[col] = max(data_w + 1, 3)
    cols = [c for c in cols if c in widths]

    # 建立多行標題
    wrapped = [_wrap_header(c, widths[c]) for c in cols]
    n_lines = max(len(w) for w in wrapped)
    # 短標題補空行至頂端對齊
    for w in wrapped:
        while len(w) < n_lines:
            w.insert(0, "")

    for i in range(n_lines):
        print("  " + " ".join(
            _pad(wrapped[j][i], widths[c], "right" if c in RIGHT else "left")
            for j, c in enumerate(cols)
        ))

    sep = "  " + "─" * (sum(widths.values()) + len(cols))
    print(sep)
    for _, row in df.iterrows():
        line = "  " + " ".join(
            _pad(str(row[c]), widths[c], "right" if c in RIGHT else "left")
            for c in cols
        )
        print(line)


def print_scan_result(rows: list, date_str: str, min_hit: int,
                      show_combos: list = None):
    print("\n" + "═"*100)
    print(f"  📋 隔日沖選股彙整  {date_str}  命中門檻：≥{min_hit}")
    if show_combos:
        print(f"  📌 組合篩選：{', '.join(show_combos)}（至少命中其中一個）")
    print("═"*100)
    print(f"  通過門檻：{len(rows)} 檔")
    print("═"*100)
    if not rows:
        print("  （今日無股票達到命中門檻）")
        return
    df = pd.DataFrame(rows)

    # 有組合篩選時：精簡欄位，只顯示有觸發的策略欄 + 命中組合
    if show_combos:
        # 收集本次所有組合涉及的策略，對應到顯示欄位名稱
        _strat_col = {
            "A":"A多","AS":"AS空","B":"B多","BS":"BS空",
            "C":"C多","CS":"CS空","D":"D多","DS":"DS空",
            "E":"E多","ES":"ES空","F":"F均量多","FS":"FS均量空",
            "G":"G潮汐多","GS":"GS潮汐空","H":"H鎚子","HS":"HS射擊",
            "I":"I吞多","IS":"IS吞空",
            # ── 新增 J~R ──
            "J":"J MACD多","JS":"JS MACD空",
            "K":"K布林多", "KS":"KS布林空",
            "L":"L KD多",  "LS":"LS KD空",
            "M":"M威廉多", "MS":"MS威廉空",
            "N":"N均線多", "NS":"NS均線空",
            "O":"O晨星",   "OS":"OS昏星",
            "P":"P三兵",   "PS":"PS三鴉",
            "Q":"Q破IB多", "QS":"QS破IB空",
            "R":"R偏低多", "RS":"RS偏高空",
        }
        involved = set()
        for c in show_combos:
            involved.update(c.split("+"))
        strat_cols = [_strat_col[s] for s in _STRAT_KEYS if s in involved]
        cols = (["代號","名稱","資料日期","收盤","漲跌幅(%)","成交量(張)","量/均量"]
                + strat_cols
                + ["命中數","方向","命中組合"])
    else:
        cols = ["代號","名稱","資料日期","收盤","漲跌幅(%)","成交量(張)","量/均量",
                "A多","AS空","B多","BS空","C多","CS空","D多","DS空","E多","ES空",
                "F均量多","FS均量空","G潮汐多","GS潮汐空",
                "H鎚子","HS射擊","I吞多","IS吞空",
                "J MACD多","JS MACD空","K布林多","KS布林空",
                "L KD多","LS KD空","M威廉多","MS威廉空",
                "N均線多","NS均線空","O晨星","OS昏星",
                "P三兵","PS三鴉","Q破IB多","QS破IB空","R偏低多","RS偏高空",
                "命中數","方向"]

    print_table(df, cols)
    print("═"*100)
    print("  A多=均線突破+爆量  AS空=均線死亡  B多=跳空↑  BS空=跳空↓")
    print("  C多=RSI超賣  CS空=RSI超買  D多=突破前高  DS空=跌破前低")
    print("  E多=強勢連漲  ES空=弱勢連跌  F多=均量擴張  FS空=均量萎縮")
    print("  G多=縮後爆量↑  GS空=縮後爆量↓  H多=鎚子K  HS空=射擊之星")
    print("  I多=吞噬陽  IS空=吞噬陰  J多=MACD金叉  JS空=MACD死叉")
    print("  K多=布林下軌反彈  KS空=布林上軌反壓  L多=KD超賣金叉  LS空=KD超買死叉")
    print("  M多=威廉%R超賣  MS空=威廉%R超買  N多=多頭排列回測  NS空=空頭排列跌破")
    print("  O多=晨星  OS空=黃昏星  P多=紅三兵  PS空=黑三兵")
    print("  Q多=InsideBar突破  QS空=InsideBar跌破  R多=BIAS超跌  RS空=BIAS超漲")
    print("═"*100 + "\n")


def print_backtest_result(stats: list, days: int, min_hit: int, stop_loss: float,
                          take_profit: float = 0.0, exit_day: int = 2,
                          short_only: bool = False, long_only: bool = False,
                          strategy_filter: list = None):
    sl_str = f"-{stop_loss}%（估算）" if stop_loss > 0 else "未設定"
    print("\n" + "═"*80)
    print(f"  📊 隔日沖回測  回測天數：{days} 日  命中門檻：≥{min_hit}  停損：{sl_str}")
    exit_str = {1:"D1收盤（隔日當沖）", 2:"D2開盤（持一日）", 3:"D3開盤（持兩日）"}.get(exit_day, f"D{exit_day}開盤")
    tp_str   = f"  止盈：+{take_profit}%（估算）" if take_profit > 0 else ""
    if strategy_filter:
        so_str = f"  │  策略篩選：{','.join(strategy_filter)}"
    elif short_only:
        so_str = "  │  只計算空方"
    elif long_only:
        so_str = "  │  只計算多方"
    else:
        so_str = ""
    print(f"  進場：D1開盤  │  出場：{exit_str}  │  手續費：{TRADE_COST*100:.3f}%{so_str}")
    print(f"  ⚠️  停損/止盈為估算值（日線 High/Low）{tp_str}")
    print("═"*80)
    df = pd.DataFrame(stats)
    # long_only 模式：只顯示多方策略
    if strategy_filter:
        # 只保留指定策略（格式如 "I｜多 吞噬陽線" 開頭的代號）
        pattern = "|".join(f"^{k}｜" for k in strategy_filter)
        df = df[df["策略"].str.match(pattern)].copy()
    elif long_only:
        df = df[~df["策略"].str.contains("空")].copy()
    elif short_only:
        df = df[~df["策略"].str.contains("多")].copy()
    cols = ["策略","訊號次數","勝率(%)","平均獲利(%)","平均虧損(%)",
            "期望值(%)","最大單筆虧損(%)","停損觸發(%)"]
    print_table(df, cols)

    # 最佳策略
    numeric = df[df["勝率(%)"] != "-"].copy()
    if not numeric.empty:
        numeric["期望值(%)"] = numeric["期望值(%)"].astype(float)
        best = numeric.loc[numeric["期望值(%)"].idxmax()]
        print(f"\n  🏆 最佳：{best['策略']}（期望值 {best['期望值(%)']:.3f}%）")
        long_best  = numeric[numeric["策略"].str.contains("多")].nlargest(1, "期望值(%)")
        short_best = numeric[numeric["策略"].str.contains("空")].nlargest(1, "期望值(%)")
        if not long_best.empty and not short_only:
            r = long_best.iloc[0]
            print(f"  📈 多方最佳：{r['策略']}（{r['期望值(%)']:.3f}%）")
        if not short_best.empty and not long_only:
            r = short_best.iloc[0]
            print(f"  📉 空方最佳：{r['策略']}（{r['期望值(%)']:.3f}%）")
    print("═"*80 + "\n")


def print_trade_detail(all_trades: list, strategy_filter: list = None,
                       long_only: bool = False, short_only: bool = False,
                       show_n: int = 20):
    """
    印出回測明細，每筆交易顯示：
      D0 收盤 → 漲/跌停價 → D1 開/高/低/收 → 出場價 → 損益
    """
    trades = all_trades
    if strategy_filter:
        trades = [t for t in trades if t["strategy"] in strategy_filter]
    if long_only:
        trades = [t for t in trades if not t["is_short"]]
    if short_only:
        trades = [t for t in trades if t["is_short"]]

    if not trades:
        print("  （無符合條件的明細交易）")
        return

    print(f"\n  🔍 回測明細（共 {len(trades)} 筆，顯示前 {min(show_n, len(trades))} 筆）")
    print("  " + "─" * 105)
    hdr = (f"  {'日期(D1)':<12} {'代號':<6} {'名稱':<8} {'策略':<4}"
           f" {'D0收盤':>7} {'漲停價':>7} {'跌停價':>7}"
           f" {'D1開':>7} {'D1高':>7} {'D1低':>7} {'D1收':>7}"
           f" {'出場價':>7} {'出場方式':<6} {'損益%':>7}")
    print(hdr)
    print("  " + "─" * 105)
    for tr in trades[:show_n]:
        direction = "空" if tr["is_short"] else "多"
        win_mark  = "✅" if tr["win"] else "❌"
        print(
            f"  {tr['date']:<12} {tr['code']:<6} {tr['name']:<8} "
            f"{tr['strategy']:<4}({direction})"
            f" {tr['d0_close']:>7.2f} {tr['lim_up']:>7.2f} {tr['lim_down']:>7.2f}"
            f" {tr['d1_open']:>7.2f} {tr['d1_high']:>7.2f} {tr['d1_low']:>7.2f} {tr['d1_close']:>7.2f}"
            f" {tr['exit_p']:>7.2f} {tr['exit_type']:<6} {tr['ret_net']:>+7.3f}% {win_mark}"
        )
    if len(trades) > show_n:
        print(f"  ... 還有 {len(trades) - show_n} 筆（使用 --show-trades N 增加顯示數量）")
    print("  " + "─" * 105)
    print(f"  驗證方式：取上表任一列，以『日期(D1)』當天去 K 線圖確認")
    print(f"    D1開盤=當日開盤價  D1高=當日最高  D1低=當日最低  D1收=當日收盤")
    print(f"    漲停價=前日收盤×1.1（捨去）  跌停價=前日收盤×0.9（進位）\n")


def print_backtest_limit_result(stats: list, days: int, min_hit: int,
                                stop_loss: float,
                                strategy_filter: list = None):
    """顯示漲跌停出場回測結果"""
    sl_str = f"-{stop_loss}%（估算）" if stop_loss > 0 else "未設定"
    print("\n" + "═"*90)
    print(f"  📊 漲跌停出場回測  回測天數：{days} 日  命中門檻：≥{min_hit}  停損：{sl_str}")
    print(f"  進場：D1 開盤  │  多方出場：漲停價 或 D1 收盤"
          f"  │  空方出場：跌停價 或 D1 收盤")
    print(f"  手續費：{TRADE_COST*100:.3f}%  │  漲跌停依台股最小升降單位精確計算（≈±10%）")
    if strategy_filter:
        print(f"  策略篩選：{','.join(strategy_filter)}")
    print("═"*90)

    df = pd.DataFrame(stats)
    if strategy_filter:
        pattern = "|".join(f"^{k}｜" for k in strategy_filter)
        df = df[df["策略"].str.match(pattern)].copy()

    cols = ["策略","訊號次數","勝率(%)","平均獲利(%)","平均虧損(%)",
            "期望值(%)","最大單筆虧損(%)","停損觸發(%)","漲跌停出場(%)"]
    print_table(df, cols)

    numeric = df[df["勝率(%)"] != "-"].copy()
    if not numeric.empty:
        numeric["期望值(%)"] = numeric["期望值(%)"].astype(float)
        best = numeric.loc[numeric["期望值(%)"].idxmax()]
        print(f"\n  🏆 最佳策略：{best['策略']}  期望值 {best['期望值(%)']:+.3f}%"
              f"  勝率 {best['勝率(%)']}%  訊號 {best['訊號次數']} 次")
        long_best  = numeric[numeric["策略"].str.contains("多")].nlargest(1, "期望值(%)")
        short_best = numeric[numeric["策略"].str.contains("空")].nlargest(1, "期望值(%)")
        if not long_best.empty:
            r = long_best.iloc[0]
            print(f"  📈 多方最佳：{r['策略']}  期望值 {r['期望值(%)']:+.3f}%  勝率 {r['勝率(%)']}%")
        if not short_best.empty:
            r = short_best.iloc[0]
            print(f"  📉 空方最佳：{r['策略']}  期望值 {r['期望值(%)']:+.3f}%  勝率 {r['勝率(%)']}%")
    print("═"*90)
    print("  ⚠️  停損以 D1 High/Low 估算；同日若同時觸及停損與漲跌停，停損優先。")
    print("  ⚠️  漲跌停出場率低不代表策略差，代表多數交易在尾盤才收盤出場。\n")


def print_history(df: pd.DataFrame):
    if df.empty:
        print("  （無歷史記錄）")
        return
    print_table(df, df.columns.tolist(), title="歷史選股記錄")


# ══════════════════════════════════════════════
# 診斷工具：比對兩個資料源
# ══════════════════════════════════════════════
def diagnose_sources(code: str, days: int,
                     sj_key: str = "", sj_secret: str = "",
                     tail: int = 10):
    """
    比對 yfinance 與 Shioaji 對同一檔股票的最近 N 根 K 棒。
    找出日期缺口、價格差異、成交量差異。
    """
    print(f"\n{'═'*70}")
    print(f"  🔬 資料源診斷：{code}  最近 {tail} 根日K")
    print(f"{'═'*70}")

    # ── 取 yfinance 資料 ──
    yf_df = None
    try:
        yf_data = fetch_data([code], days)
        if code in yf_data:
            yf_df = yf_data[code].tail(tail)[["Open","High","Low","Close","Vol_K"]].copy()
            yf_df.index = pd.to_datetime(yf_df.index).date
    except Exception as e:
        print(f"  ⚠️  yfinance 取得失敗：{e}")

    # ── 取 Shioaji 資料 ──
    sj_df = None
    if sj_key and sj_secret:
        try:
            sj_data = fetch_data_sinopac([code], days, sj_key, sj_secret)
            if code in sj_data:
                sj_df = sj_data[code].tail(tail)[["Open","High","Low","Close","Vol_K"]].copy()
                sj_df.index = pd.to_datetime(sj_df.index).date
        except Exception as e:
            print(f"  ⚠️  Shioaji 取得失敗：{e}")
    else:
        print(f"  ℹ️  未提供 Shioaji 憑證，只顯示 yfinance 資料")

    # ── 比對輸出 ──
    if yf_df is not None:
        print(f"\n  【yfinance】最後一筆日期：{yf_df.index[-1]}  收盤：{yf_df['Close'].iloc[-1]}")
        print(f"  {'日期':<12} {'開':>7} {'高':>7} {'低':>7} {'收':>7} {'量(張)':>9}")
        print(f"  {'─'*55}")
        for dt, row in yf_df.iterrows():
            print(f"  {str(dt):<12} {row['Open']:>7.1f} {row['High']:>7.1f} "
                  f"{row['Low']:>7.1f} {row['Close']:>7.1f} {row['Vol_K']:>9.0f}")

    if sj_df is not None:
        print(f"\n  【Shioaji】最後一筆日期：{sj_df.index[-1]}  收盤：{sj_df['Close'].iloc[-1]}")
        print(f"  {'日期':<12} {'開':>7} {'高':>7} {'低':>7} {'收':>7} {'量(張)':>9}")
        print(f"  {'─'*55}")
        for dt, row in sj_df.iterrows():
            print(f"  {str(dt):<12} {row['Open']:>7.1f} {row['High']:>7.1f} "
                  f"{row['Low']:>7.1f} {row['Close']:>7.1f} {row['Vol_K']:>9.0f}")

    # ── 差異摘要 ──
    if yf_df is not None and sj_df is not None:
        common = sorted(set(yf_df.index) & set(sj_df.index))
        yf_only = sorted(set(yf_df.index) - set(sj_df.index))
        sj_only = sorted(set(sj_df.index) - set(yf_df.index))
        print(f"\n  📊 差異摘要：")
        print(f"     共同日期：{len(common)} 天")
        if yf_only:
            print(f"     ⚠️  yfinance 有但 Shioaji 沒有：{yf_only}")
        if sj_only:
            print(f"     ⚠️  Shioaji 有但 yfinance 沒有：{sj_only}")
        if common:
            max_close_diff = max(
                abs(yf_df.loc[d, "Close"] - sj_df.loc[d, "Close"])
                for d in common if d in yf_df.index and d in sj_df.index
            )
            max_vol_ratio = max(
                yf_df.loc[d, "Vol_K"] / sj_df.loc[d, "Vol_K"]
                if sj_df.loc[d, "Vol_K"] > 0 else 0
                for d in common if d in yf_df.index and d in sj_df.index
            )
            print(f"     最大收盤價差：{max_close_diff:.2f} 元")
            print(f"     yfinance/Shioaji 成交量比：{max_vol_ratio:.3f}x"
                  f"  {'（接近 1 = 單位相同）' if 0.9 < max_vol_ratio < 1.1 else '（⚠️ 單位可能不同）'}")

    print(f"{'═'*70}\n")


# ══════════════════════════════════════════════
# 主程式
# ══════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="台股隔日沖選股與回測系統",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例：
  python swing_trade.py                         # 選股（預設，等同 --scan）
  python swing_trade.py --min-hit 2 --save      # 選股，命中 ≥2，儲存結果
  python swing_trade.py --backtest --days 60    # 回測 60 日
  python swing_trade.py --history --limit 10   # 查詢最近 10 筆歷史
  python swing_trade.py --history --code 2330  # 查詢 2330 歷史
        """,
    )

    # ── 執行模式 ──────────────────────────────────
    mode_group = parser.add_argument_group("執行模式（預設：--scan）")
    mode_ex = mode_group.add_mutually_exclusive_group()
    mode_ex.add_argument("--scan",           action="store_true",
                         help="執行選股（預設，無參數時自動啟用）")
    mode_ex.add_argument("--backtest",       action="store_true",
                         help="執行回測（D+N 開盤出場）")
    mode_ex.add_argument("--backtest-limit", action="store_true",
                         help="執行漲跌停出場回測（D1 盤中漲停/跌停 或 D1 收盤出場）")
    mode_ex.add_argument("--combo",          action="store_true",
                         help="策略組合分析：計算所有兩策略雙重確認的勝率與期望值")
    mode_ex.add_argument("--history",        action="store_true",
                         help="查詢歷史選股記錄")
    mode_ex.add_argument("--diagnose",       type=str, default=None,
                         metavar="CODE",
                         help="診斷：比對兩個資料源對指定股票的 K 棒差異，如 --diagnose 4540")

    # ── 選股 / 量能篩選 ───────────────────────────
    scan_group = parser.add_argument_group("選股 / 量能篩選")
    scan_group.add_argument("--top-n",   type=int,   default=300,
                            help="掃描成交量前 N 名，預設 300")
    scan_group.add_argument("--min-vol", type=int,   default=MIN_AVG_VOL,
                            help=f"5日均量最低門檻（張），預設 {MIN_AVG_VOL}")
    scan_group.add_argument("--vol-mult",type=float, default=VOL_MULT,
                            help=f"爆量倍數，預設 {VOL_MULT}")
    scan_group.add_argument("--min-hit", type=int,   default=1,
                            help="策略命中門檻，預設 1，建議設 2")
    scan_group.add_argument("--save",    action="store_true",
                            help="儲存選股結果到 SQLite 和 CSV")

    # ── 進出場設定 ────────────────────────────────
    trade_group = parser.add_argument_group("進出場設定（選股 / 回測共用）")
    trade_group.add_argument("--stop-loss",   type=float, default=2.0,
                             help="停損門檻（%%），0=不設停損，預設 2.0（估算值）")
    trade_group.add_argument("--take-profit", type=float, default=0.0,
                             help="止盈門檻（%%），0=不設止盈，如 2.0=漲 2%% 出場（估算值）")
    trade_group.add_argument("--exit-day",    type=int,   default=2,
                             help="出場日：1=隔日當沖(D1收盤)  2=持一日(D2開盤，預設)  3=持兩日(D3開盤)")

    # ── 策略篩選 ──────────────────────────────────
    strat_group = parser.add_argument_group("策略篩選")
    strat_ex = strat_group.add_mutually_exclusive_group()
    strat_ex.add_argument("--long-only",  action="store_true",
                          help="只顯示多方策略（市場偏多時適用）")
    strat_ex.add_argument("--short-only", action="store_true",
                          help="只顯示空方策略（市場偏空時適用）")
    strat_group.add_argument("--strategy", type=str, default=None,
                             help="只看指定策略，逗號分隔，如 I,D,C 或 I,BS（大小寫均可）")

    # ── 回測專用 ──────────────────────────────────
    bt_group = parser.add_argument_group("回測專用")
    bt_group.add_argument("--days", type=int, default=DEFAULT_DAYS,
                          help=f"回測天數，預設 {DEFAULT_DAYS}")
    bt_group.add_argument("--show-trades", type=int, default=0, metavar="N",
                          help="顯示前 N 筆個別交易明細（搭配 --backtest-limit 使用，"
                               "用於人工驗證回測邏輯，如 --show-trades 20）")
    bt_group.add_argument("--min-signals", type=int, default=10, metavar="N",
                          help="組合分析：低於 N 筆的組合標記為樣本不足（預設 10）")
    bt_group.add_argument("--max-combo",  type=int, default=3, metavar="N",
                          help="組合分析：最大組合策略數（預設 3；設 2 只看兩兩配對）")
    bt_group.add_argument("--workers",     type=int, default=4, metavar="N",
                          help="回測平行執行緒數（預設 4；設 1 停用多執行緒）")
    bt_group.add_argument("--show-combos", type=str, default=None, metavar="COMBOS",
                          help="只顯示指定組合（逗號分隔），"
                               "如 --show-combos \"BS+GS,AS+BS+GS\"")
    bt_group.add_argument("--preset",      type=str, default=None, metavar="NAME",
                          help=f"使用預設組合清單（等同 --show-combos 展開版），"
                               f"可用：{', '.join(COMBO_PRESETS.keys())}")

    # ── 資料來源 ──────────────────────────────────
    src_group = parser.add_argument_group("資料來源")
    src_group.add_argument("--datasource", type=str, default="sinopac",
                           choices=["yfinance", "sinopac"],
                           help="行情資料來源：sinopac（預設，永豐金，盤後 5 分鐘即更新，需帳號）"
                                " 或 yfinance（免帳號）")
    src_group.add_argument("--sj-api-key",    type=str, default=None,
                           metavar="KEY",
                           help="永豐金 API Key（亦可設環境變數 SJ_API_KEY）")
    src_group.add_argument("--sj-secret-key", type=str, default=None,
                           metavar="SECRET",
                           help="永豐金 Secret Key（亦可設環境變數 SJ_SECRET_KEY）")
    src_ex = src_group.add_mutually_exclusive_group()
    src_ex.add_argument("--codes", type=str, default=None,
                        help="指定股票代號，逗號分隔，如 2330,2317,2454")
    src_ex.add_argument("--csv",   type=str, default=None,
                        help="從選股 CSV 載入代號，如 --csv 隔日沖選股_20260415.csv")

    # ── 歷史查詢 ──────────────────────────────────
    hist_group = parser.add_argument_group("歷史查詢（搭配 --history）")
    hist_group.add_argument("--code",  type=str, default=None,
                            help="查詢特定股票的歷史記錄")
    hist_group.add_argument("--limit", type=int, default=20,
                            help="查詢筆數，預設 20")

    args = parser.parse_args()

    # 預設模式：無其他模式時自動啟用 --scan
    if not any([args.scan, args.backtest, args.backtest_limit,
                args.combo, args.history, args.diagnose]):
        args.scan = True

    # ── --preset 展開（優先於 --show-combos）────────────────────
    if args.preset:
        expanded = resolve_preset(args.preset)
        if not expanded:
            sys.exit(1)
        if args.show_combos:
            print(f"  ℹ️  --preset 與 --show-combos 同時指定，以 --preset {args.preset} 為準")
        args.show_combos = ",".join(expanded)

    # 啟動提示
    date_str = datetime.date.today().strftime("%Y/%m/%d")
    print("\n" + "═"*65)
    print(f"  📈 台股隔日沖選股與回測系統  {date_str}")
    print("═"*65)

    # 初始化資料庫
    conn = init_db()

    # ── 歷史查詢 ─────────────────────────────────
    if args.history:
        print(f"  查詢歷史選股記錄（最近 {args.limit} 筆）")
        df = query_history(conn, code=args.code, limit=args.limit)
        print_history(df)
        conn.close()
        return

    # ── 資料源診斷 ────────────────────────────────
    if args.diagnose:
        sj_key    = args.sj_api_key    or os.environ.get("SJ_API_KEY",    "")
        sj_secret = args.sj_secret_key or os.environ.get("SJ_SECRET_KEY", "")
        diagnose_sources(args.diagnose, days=60,
                         sj_key=sj_key, sj_secret=sj_secret, tail=10)
        conn.close()
        return

    # ── 下載資料 ─────────────────────────────────
    if args.codes or args.csv:
        src_str = f"自訂清單（--codes/--csv）"
    else:
        src_str = f"成交量前 {args.top_n} 名"
    ds_str = "永豐金 Shioaji" if args.datasource == "sinopac" else "yfinance"
    print(f"  掃描範圍：{src_str}  │  最低均量：{args.min_vol} 張  │  資料源：{ds_str}")
    print(f"  命中門檻：≥{args.min_hit}  │  爆量倍數：{args.vol_mult}x")
    if args.backtest:
        sl_str  = f"-{args.stop_loss}%（估算）" if args.stop_loss > 0 else "未設定"
        tp_str  = f"  止盈：+{args.take_profit}%" if args.take_profit > 0 else ""
        if args.short_only:
            so_str = "  只算空方"
        elif args.long_only:
            so_str = "  只算多方"
        else:
            so_str = ""
        ed_map  = {1:"隔日當沖",2:"持一日",3:"持兩日"}
        ed_str  = ed_map.get(args.exit_day, f"D{args.exit_day}")
        print(f"  回測天數：{args.days} 日  │  出場：{ed_str}  │  停損：{sl_str}{tp_str}{so_str}")
    if args.backtest_limit:
        sl_str = f"-{args.stop_loss}%（估算）" if args.stop_loss > 0 else "未設定"
        print(f"  回測天數：{args.days} 日  │  出場：漲停/跌停 或 D1收盤  │  停損：{sl_str}")
    print(f"  平行執行緒：{args.workers} 個（可用 --workers N 調整）")
    print("═"*65 + "\n")

    # 優先用 --codes / --csv，否則從 TWSE 取清單
    codes = []
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        print(f"  📋 自訂清單：{len(codes)} 檔")
    elif args.csv:
        try:
            df_csv = pd.read_csv(args.csv)
            col    = next((c for c in df_csv.columns if "代號" in c), df_csv.columns[0])
            codes  = df_csv[col].astype(str).str.strip().tolist()
            print(f"  📋 從 CSV 載入：{len(codes)} 檔（{args.csv}）")
        except Exception as e:
            print(f"  ❌ CSV 讀取失敗：{e}")
            conn.close()
            sys.exit(1)
    else:
        codes = _get_top_codes(args.top_n)

    if not codes:
        print("  ❌ 無法取得股票清單，請使用 --codes 2330,2317 指定股票")
        conn.close()
        sys.exit(1)

    # ── 選擇資料來源 ──────────────────────────────
    if args.datasource == "sinopac":
        sj_key    = args.sj_api_key    or os.environ.get("SJ_API_KEY",    "")
        sj_secret = args.sj_secret_key or os.environ.get("SJ_SECRET_KEY", "")
        if not sj_key or not sj_secret:
            print("  ❌ 使用永豐金資料源需提供 API 金鑰，有兩種方式：")
            print("     方式一（命令列）：--sj-api-key YOUR_KEY --sj-secret-key YOUR_SECRET")
            print("     方式二（環境變數）：set SJ_API_KEY=... && set SJ_SECRET_KEY=...")
            print("     ＊ API 金鑰請至永豐金證券後台申請")
            conn.close()
            sys.exit(1)
        data = fetch_data_sinopac(codes, args.days + 30, sj_key, sj_secret)
    else:
        data = fetch_data(codes, args.days + 30)

    if not data:
        print("  ❌ 無法取得歷史資料")
        conn.close()
        sys.exit(1)

    # ── 選股 ─────────────────────────────────────
    if args.scan:
        strat_filter = [s.strip().upper() for s in args.strategy.split(",")] if args.strategy else None
        show_combos  = ([c.strip() for c in args.show_combos.split(",") if c.strip()]
                        if args.show_combos else None)
        rows = run_scan(data, args.min_hit, args.vol_mult, args.min_vol,
                        strategy_filter=strat_filter,
                        show_combos=show_combos,
                        workers=args.workers)
        print_scan_result(rows, date_str, args.min_hit, show_combos=show_combos)

        if args.save and rows:
            scan_date = datetime.date.today().isoformat()
            save_scan(conn, scan_date, rows, args.min_hit)
            print(f"  💾 選股結果已存入 SQLite：{DB_PATH}")

            fname = f"隔日沖選股_{datetime.date.today().strftime('%Y%m%d')}.csv"
            pd.DataFrame(rows).to_csv(fname, index=False, encoding="utf-8-sig")
            print(f"  💾 CSV：{fname}")

    # ── 回測 ─────────────────────────────────────
    if args.backtest:
        strat_filter = [s.strip().upper() for s in args.strategy.split(",")] \
                       if args.strategy else None
        stats = run_backtest(data, args.days, args.min_hit,
                             args.vol_mult, args.min_vol, args.stop_loss,
                             take_profit=args.take_profit,
                             exit_day=args.exit_day,
                             short_only=args.short_only,
                             long_only=args.long_only,
                             strategy_filter=strat_filter,
                             workers=args.workers)
        print_backtest_result(stats, args.days, args.min_hit, args.stop_loss,
                              take_profit=args.take_profit,
                              exit_day=args.exit_day,
                              short_only=args.short_only,
                              long_only=args.long_only,
                              strategy_filter=strat_filter)

        if args.save:
            ed_tag = f"_d{args.exit_day}"
            tp_tag = f"_tp{args.take_profit}" if args.take_profit > 0 else ""
            so_tag = "_short" if args.short_only else ""
            fname  = save_backtest_csv(stats, args.days, args.min_hit, args.stop_loss)
            print(f"  💾 回測結果已儲存：{fname}")

    # ── 漲跌停出場回測 ───────────────────────────
    if args.backtest_limit:
        strat_filter = [s.strip().upper() for s in args.strategy.split(",")]\
                       if args.strategy else None
        need_trades  = args.show_trades > 0

        result = run_backtest_limit(
            data, args.days, args.min_hit,
            args.vol_mult, args.min_vol,
            stop_loss=args.stop_loss,
            strategy_filter=strat_filter,
            return_trades=need_trades,
            workers=args.workers,
        )
        if need_trades:
            stats, all_trades = result
        else:
            stats = result

        print_backtest_limit_result(
            stats, args.days, args.min_hit, args.stop_loss,
            strategy_filter=strat_filter,
        )

        if need_trades:
            print_trade_detail(
                all_trades,
                strategy_filter=strat_filter,
                long_only=args.long_only,
                short_only=args.short_only,
                show_n=args.show_trades,
            )
        if args.save:
            sl_tag = f"_sl{args.stop_loss}" if args.stop_loss > 0 else "_nosl"
            date_s = datetime.date.today().strftime("%Y%m%d")
            fname  = f"漲跌停回測_{date_s}_d{args.days}_hit{args.min_hit}{sl_tag}.csv"
            df_out = pd.DataFrame(stats)
            df_out.insert(0, "回測日期", datetime.date.today().isoformat())
            df_out.insert(1, "回測天數", args.days)
            df_out.insert(2, "命中門檻", args.min_hit)
            df_out.insert(3, "停損(%)",  args.stop_loss)
            df_out.to_csv(fname, index=False, encoding="utf-8-sig")
            print(f"  💾 回測結果已儲存：{fname}")

    # ── 策略組合分析 ─────────────────────────────
    if args.combo:
        direction = "short" if args.short_only else "long"
        rows = run_combo_analysis(
            data, args.days, args.min_hit,
            args.vol_mult, args.min_vol,
            stop_loss=args.stop_loss,
            direction=direction,
            min_signals=args.min_signals,
            max_combo=args.max_combo,
            workers=args.workers,
        )
        show_combos = ([c.strip() for c in args.show_combos.split(",") if c.strip()]
                       if args.show_combos else None)
        print_combo_result(rows, args.days, direction,
                           args.stop_loss, args.min_signals,
                           max_combo=args.max_combo,
                           show_combos=show_combos)
        if args.save and rows:
            dir_tag = "short" if args.short_only else "long"
            sl_tag  = f"_sl{args.stop_loss}" if args.stop_loss > 0 else "_nosl"
            date_s  = datetime.date.today().strftime("%Y%m%d")
            fname   = f"組合分析_{dir_tag}_{date_s}_d{args.days}{sl_tag}.csv"
            pd.DataFrame(rows).to_csv(fname, index=False, encoding="utf-8-sig")
            print(f"  💾 組合分析已儲存：{fname}")

    conn.close()


if __name__ == "__main__":
    main()
