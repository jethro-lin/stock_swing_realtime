"""
台股隔日沖選股 Web App
啟動方式：python web_app.py
依賴：pip install fastapi uvicorn
瀏覽器開啟：http://localhost:8080
"""
from __future__ import annotations
import asyncio
import datetime
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ─────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DB_PATH    = BASE_DIR / "swing_trade.db"
SCRIPT     = BASE_DIR / "swing_trade_v2.py"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="台股選股系統", docs_url=None, redoc_url=None)

SIGNAL_DESC: dict[str, str] = {
    "A": "均線突破",      "AS": "均線跌破",
    "B": "跳空↑",         "BS": "跳空↓",
    "B2": "大跳空≥5%",    "C":  "RSI超賣",      "CS": "RSI超買",
    "D": "突破前高",      "DS": "跌破前低",
    "E": "強勢連漲",      "ES": "強勢連跌",
    "F": "均量擴張",      "FS": "量縮空",
    "G": "縮後上漲",      "GS": "縮後下跌",
    "H": "鎚子K",         "HS": "吊人線",
    "I": "吞噬陽",        "IS": "吞噬陰",
    "J": "MACD金叉",      "JS": "MACD死叉",
    "K": "布林下軌",      "KS": "布林上軌",
    "L": "KD<20金叉",     "LS": "KD>80死叉",
    "M": "威廉超賣",      "MS": "威廉超買",
    "N": "多頭排列回測",  "NS": "空頭排列",
    "O": "晨星",          "OS": "夜星",
    "P": "紅三兵",        "PS": "黑三兵",
    "Q": "IB突破",        "QS": "IB跌破",
    "R": "BIAS<-10%反彈", "RS": "BIAS>+10%回落",
    "T": "W底頸線突破",   "TS": "M頭頸線跌破",
}

PRESETS = [
    "long3_lean,short3_lean,long3_pattern,long_momentum",
    "long3_lean",
    "short3_lean",
    "long3_pattern",
    "long_momentum",
    "long3",
    "short3",
    "all3_lean",
    "allforall",
]

_scan_running = False   # 防止重複觸發

# ─── helpers ─────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ─── API routes ──────────────────────────────────────────────

@app.get("/api/dates")
def get_dates():
    conn = _db()
    rows = conn.execute(
        "SELECT DISTINCT scan_date FROM scans ORDER BY scan_date DESC LIMIT 90"
    ).fetchall()
    conn.close()
    return {"dates": [r["scan_date"] for r in rows]}


@app.get("/api/results/{date}")
def get_results(date: str):
    conn = _db()
    rows = conn.execute(
        """SELECT code, name, close, chg_pct, vol_k, vol_ratio,
                  strategies, hit_count, direction
           FROM scans
           WHERE scan_date = ?
           ORDER BY hit_count DESC, ABS(chg_pct) DESC""",
        (date,),
    ).fetchall()
    conn.close()

    # 同一天可能跑多次（不同 preset / --refresh-cache），同代號保留最新一筆（MAX id）
    # 注意：不用 hit_count 排序，因為舊的 --refresh-cache 跑可能用昨日資料產生較高命中
    seen: dict = {}
    for r in rows:
        code = r["code"]
        if code not in seen:
            seen[code] = dict(r)   # sqlite3.Row → 轉 dict
    # rows 已按 id ASC（fetchall 順序）→ 再遍歷一次取最新 id
    # 重新用 MAX(id) 查詢，確保每個 code 只取最後一次寫入的資料
    conn2 = _db()
    latest_rows = conn2.execute(
        """SELECT s.code, s.name, s.close, s.chg_pct, s.vol_k, s.vol_ratio,
                  s.strategies, s.hit_count, s.direction
           FROM scans s
           INNER JOIN (
               SELECT code, MAX(id) AS max_id
               FROM scans
               WHERE scan_date = ?
               GROUP BY code
           ) best ON s.id = best.max_id""",
        (date,),
    ).fetchall()
    conn2.close()

    results = sorted(
        [
            {
                "code":       r["code"],
                "name":       r["name"],
                "close":      r["close"],
                "chg_pct":    r["chg_pct"],
                "vol_k":      r["vol_k"],
                "vol_ratio":  r["vol_ratio"],
                "strategies": r["strategies"].split(",") if r["strategies"] else [],
                "hit_count":  r["hit_count"],
                "direction":  r["direction"],
            }
            for r in latest_rows
        ],
        key=lambda x: (-x["hit_count"], -abs(x["chg_pct"])),
    )
    return {"date": date, "results": results, "total": len(results)}


@app.get("/api/kline/{code}")
def get_kline(code: str, days: int = 180):
    conn = _db()
    start = (datetime.date.today() - datetime.timedelta(days=days + 40)).isoformat()
    bars = conn.execute(
        """SELECT date, open, high, low, close, vol_k
           FROM kbars_daily
           WHERE code = ? AND date >= ?
           ORDER BY date ASC""",
        (code, start),
    ).fetchall()
    sigs = conn.execute(
        """SELECT scan_date, strategies, direction
           FROM scans
           WHERE code = ?
           ORDER BY scan_date ASC""",
        (code,),
    ).fetchall()
    # stock name
    name_row = conn.execute(
        "SELECT name FROM scans WHERE code = ? LIMIT 1", (code,)
    ).fetchone()
    conn.close()

    return {
        "code": code,
        "name": name_row["name"] if name_row else code,
        "bars": [
            {"time": r["date"], "open": r["open"], "high": r["high"],
             "low": r["low"], "close": r["close"], "vol": r["vol_k"]}
            for r in bars
        ],
        "signals": [
            {"date": s["scan_date"],
             "signals": s["strategies"].split(",") if s["strategies"] else [],
             "direction": s["direction"]}
            for s in sigs
        ],
    }


@app.get("/api/signal_desc")
def signal_desc():
    return SIGNAL_DESC


@app.get("/api/presets")
def get_presets():
    return {"presets": PRESETS}


@app.get("/api/scan/run")
async def scan_run(preset: str = "long3_lean,short3_lean,long3_pattern,long_momentum",
                   refresh: bool = False):
    """SSE：串流輸出掃描 log。"""
    global _scan_running

    async def event_gen():
        global _scan_running
        if _scan_running:
            yield "data: ⚠️  已有掃描正在執行中，請稍後再試\n\n"
            yield "data: __DONE__1\n\n"
            return

        cmd = [sys.executable, str(SCRIPT), "--scan", "--save",
               "--preset", preset]
        if refresh:
            cmd.append("--refresh-cache")

        _scan_running = True
        env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(BASE_DIR),
                env=env,
            )
            yield f"data: 🚀 執行中：{' '.join(cmd[2:])}\n\n"
            async for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").rstrip()
                if line:
                    # SSE data cannot contain newlines — already one line
                    yield f"data: {line}\n\n"
            await proc.wait()
            rc = proc.returncode
            if rc == 0:
                yield "data: ✅ 掃描完成\n\n"
            else:
                yield f"data: ❌ 掃描失敗（exit {rc}）\n\n"
            yield f"data: __DONE__{rc}\n\n"
        except Exception as exc:
            yield f"data: ❌ 錯誤：{exc}\n\n"
            yield "data: __DONE__1\n\n"
        finally:
            _scan_running = False

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Static / fallback ───────────────────────────────────────

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/{full_path:path}")
def catch_all(full_path: str):
    return FileResponse(STATIC_DIR / "index.html")


# ─── Entry point ─────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    print("=" * 55)
    print("  台股選股系統 Web App")
    print("  http://localhost:8080")
    print("=" * 55)
    uvicorn.run("web_app:app", host="0.0.0.0", port=8080, reload=False)
