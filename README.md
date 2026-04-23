# 台股隔日沖 & 日當沖系統

> 以永豐金 Shioaji API 為主要資料源，支援盤後選股、策略回測、盤中即時監控。

---

## 專案結構

| 檔案 | 說明 |
|------|------|
| `swing_trade.py` | 隔日沖選股 & 回測系統（盤後執行） |
| `daytrade_live.py` | 日當沖即時監控系統（盤中執行） |

---

## 環境需求

### Python 版本
Python 3.8 以上

### 安裝套件

```bash
# 必要
pip install shioaji pandas numpy requests

# 選用（yfinance 作為備援資料源）
pip install yfinance

# 選用（Windows 桌面通知）
pip install winotify
```

### 環境變數（永豐金 API 憑證）

使用永豐金資料源前，需設定以下環境變數：

```powershell
# PowerShell（永久設定）
[System.Environment]::SetEnvironmentVariable("SJ_API_KEY", "你的API_KEY", "User")
[System.Environment]::SetEnvironmentVariable("SJ_SECRET_KEY", "你的SECRET_KEY", "User")
```

> 設定後需重新開啟終端機才會生效。API Key 請至 [永豐金證券官網](https://www.sinotrade.com.tw/) 申請。

---

## swing_trade.py — 隔日沖選股 & 回測

### 功能

- **選股**：收盤後掃描全市場，輸出隔日候選股（終端機 + CSV）
- **回測**：對歷史資料跑策略，計算勝率 / 期望值 / 最大回撤
- **歷史查詢**：從 SQLite 查詢過去選股紀錄與實際損益
- **策略組合分析**：計算所有策略兩兩組合的統計表現

### 十六個策略

| 代號 | 方向 | 說明 |
|------|------|------|
| A | 多 | 均線突破 + 爆量 |
| AS | 空 | 均線死亡 + 爆量 |
| B | 多 | 開盤跳空向上 |
| BS | 空 | 開盤跳空向下 |
| C | 多 | RSI 超賣反彈 |
| CS | 空 | RSI 超買反轉 |
| D | 多 | 突破近 5 日高點 |
| DS | 空 | 跌破前低 |
| E | 多 | 強勢連漲 |
| ES | 空 | 弱勢連跌 |
| F | 多 | 均量擴張（近5日均量 > 20日均量 × 1.2 且爆量上漲） |
| FS | 空 | 均量萎縮（近5日均量 < 20日均量 × 0.8 且爆量下跌） |
| G | 多 | 量能潮汐（連續3日縮量後今日放量上漲） |
| GS | 空 | 量能枯竭（連續3日縮量後今日放量下跌） |
| H | 多 | 鎚子K（下影線 ≥ 2×實體，前日下跌） |
| HS | 空 | 射擊之星（上影線 ≥ 2×實體，前日上漲） |
| I | 多 | 吞噬多（大陽線吞噬前日陰線） |
| IS | 空 | 吞噬空（大陰線吞噬前日陽線） |

### 進出場邏輯

- **進場**：今日收盤訊號成立 → 明日開盤價買入（多）或融券賣出（空）
- **出場**：依 `--exit-day` 設定（預設 D+2 開盤）
- **停損**：用明日 Low（多）/ High（空）估算是否觸損

### 常用指令

```bash
# 選股（盤後執行，建議門檻 2）
python swing_trade.py --scan --min-hit 2 --save

# 回測（60 日，停損 2%）
python swing_trade.py --backtest --days 60 --min-hit 2 --stop-loss 2.0

# 漲跌停出場回測
python swing_trade.py --backtest-limit --days 60 --stop-loss 1.5

# 策略組合分析
python swing_trade.py --combo --days 60

# 只看特定組合結果
python swing_trade.py --scan --show-combos "B+D,A+B+E"

# 查詢最近 20 筆歷史選股
python swing_trade.py --history --limit 20

# 查詢特定股票歷史
python swing_trade.py --history --code 2330

# 診斷資料源差異
python swing_trade.py --diagnose 2330

# 使用 yfinance（不需帳號）
python swing_trade.py --scan --datasource yfinance
```

### 完整參數說明

#### 執行模式（擇一）

| 參數 | 說明 |
|------|------|
| `--scan` | 執行選股（預設，無參數時自動啟用） |
| `--backtest` | 執行回測（D+N 開盤出場） |
| `--backtest-limit` | 漲跌停出場回測 |
| `--combo` | 策略組合分析（所有兩策略雙重確認） |
| `--history` | 查詢歷史選股記錄 |
| `--diagnose CODE` | 診斷指定股票的兩資料源 K 棒差異 |

#### 選股 / 量能篩選

| 參數 | 預設 | 說明 |
|------|------|------|
| `--top-n N` | 300 | 掃描成交量前 N 名 |
| `--min-vol N` | 3000 | 5日均量最低門檻（張） |
| `--vol-mult X` | 1.5 | 爆量倍數 |
| `--min-hit N` | 1 | 策略命中門檻（建議設 2） |
| `--save` | - | 儲存結果到 SQLite 和 CSV |

#### 進出場設定

| 參數 | 預設 | 說明 |
|------|------|------|
| `--stop-loss X` | 2.0 | 停損門檻（%），0 = 不設停損 |
| `--take-profit X` | 0.0 | 止盈門檻（%），0 = 不設止盈 |
| `--exit-day N` | 2 | 出場日：1=隔日當沖 / 2=持一日（D2開盤）/ 3=持兩日 |

#### 策略篩選

| 參數 | 說明 |
|------|------|
| `--long-only` | 只顯示多方策略 |
| `--short-only` | 只顯示空方策略 |
| `--strategy A,B,C` | 只看指定策略（逗號分隔，大小寫均可） |

#### 回測專用

| 參數 | 預設 | 說明 |
|------|------|------|
| `--days N` | 60 | 回測天數 |
| `--show-trades N` | 0 | 顯示前 N 筆個別交易明細 |
| `--min-signals N` | 10 | 組合分析：低於 N 筆標記為樣本不足 |
| `--max-combo N` | 3 | 組合分析：最大策略組合數 |
| `--workers N` | 4 | 平行執行緒數 |
| `--show-combos "X+Y,..."` | - | 只顯示指定組合 |
| `--preset NAME` | - | 使用預設組合清單 |

#### 資料來源

| 參數 | 預設 | 說明 |
|------|------|------|
| `--datasource` | sinopac | `sinopac`（永豐金）或 `yfinance`（免帳號） |
| `--sj-api-key KEY` | - | 永豐金 API Key（或設環境變數 `SJ_API_KEY`） |
| `--sj-secret-key SECRET` | - | 永豐金 Secret Key（或設環境變數 `SJ_SECRET_KEY`） |
| `--codes 2330,2317` | - | 指定股票代號（逗號分隔） |
| `--csv FILE` | - | 從選股 CSV 載入代號 |

#### 歷史查詢

| 參數 | 預設 | 說明 |
|------|------|------|
| `--code CODE` | - | 查詢特定股票的歷史記錄 |
| `--limit N` | 20 | 查詢筆數 |

---

## daytrade_live.py — 日當沖即時監控

### 功能

- 以 Shioaji Tick 推送為基礎（有成交才觸發，不用輪詢）
- 盤前預算技術指標基準，盤中固定不變
- 即時顯示訊號命中狀態、進出場建議
- 觸發訊號時發送 Windows 桌面通知 + 聲音提示

### 常用指令

```bash
# 從昨日選股 CSV 載入監控清單
python daytrade_live.py --csv 隔日沖選股_20260416.csv

# 指定股票清單
python daytrade_live.py --codes 2330,2317,2454

# 設定命中門檻與停損
python daytrade_live.py --csv xxx.csv --min-hit 2 --stop-loss 1.5

# 關閉通知與聲音
python daytrade_live.py --csv xxx.csv --no-notify --no-sound
```

### 完整參數說明

| 參數 | 預設 | 說明 |
|------|------|------|
| `--codes 2330,2317` | - | 股票代號（逗號分隔），與 `--csv` 擇一 |
| `--csv FILE` | - | 從選股 CSV 載入候選股，與 `--codes` 擇一 |
| `--min-hit N` | 1 | 策略命中門檻（建議設 2） |
| `--stop-loss X` | 1.5 | 停損門檻（%） |
| `--vol-mult X` | 1.5 | 爆量倍數 |
| `--refresh N` | 5 | 畫面刷新間隔（秒） |
| `--no-notify` | - | 關閉 Windows 桌面通知 |
| `--no-sound` | - | 關閉聲音提示 |

### 注意事項

- 僅支援 **Windows**（通知功能使用 `winsound` / `winotify`）
- 交易時段：09:00 ~ 13:30
- 需先完成 `SJ_API_KEY` 與 `SJ_SECRET_KEY` 環境變數設定

---

## 典型工作流程

```
盤後（收盤後）
  └─ python swing_trade.py --scan --min-hit 2 --save
       └─ 產出 CSV：隔日沖選股_YYYYMMDD.csv

隔日盤中（開盤前）
  └─ python daytrade_live.py --csv 隔日沖選股_YYYYMMDD.csv --min-hit 2
       └─ 即時監控，訊號成立時通知
```

---

## 資料儲存

- `swing_trade.db`：SQLite，與腳本同目錄，自動建立
  - `scans` 資料表：每次選股結果
  - `backtests` 資料表：回測結果
- `隔日沖選股_YYYYMMDD.csv`：選股當日產出，可直接餵給 `daytrade_live.py`
