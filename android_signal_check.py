#!/usr/bin/env python3
"""
android_signal_check.py — 對齊 Android/Kotlin 邏輯的訊號驗證工具

用法：
  python android_signal_check.py 2330          # 驗證單股
  python android_signal_check.py 2330 2317 1303 # 多股
  python android_signal_check.py --scan        # 全市場 (取前50支有訊號的)
"""
import sys, warnings, datetime, math, requests
warnings.filterwarnings("ignore")
sys.path.insert(0, ".")

COMBO_PRESETS = {
    "long3_lean": [
        ["M","R","B2"],["R","B2"],["B","K","R"],["B","K","L"],["B2","K"],
        ["B","F","L"],["B","F","K"],["B","L","R"],["B","F","R"],["K","L","R"],
        ["B","M","R"],["B","R"],["B","K"],["K","R"],["B2","M"],
    ],
    "long_trend": [
        ["B2","N","F"],["B2","F"],["B2","A","D"],["B2","A"],["B2","D"],["B2","N"],
        ["B","N","F"],["B","E","F"],["B","F"],["B","E","D"],["B","E"],["B","N"],
        ["B","A","D"],["B","N","D"],["B","A"],["B","D"],
    ],
    "short3_lean": [
        ["BS","GS"],["BS","MS","RS"],["BS","RS"],["BS","FS","NS"],
        ["BS","FS","JS"],["AS","DS","GS"],["FS","RS"],["BS","KS"],["BS","OS"],
        ["BS","DS","NS"],["CS","FS","JS"],["AS","BS","DS"],["BS","CS","JS"],["BS","MS"],
    ],
}

# ── Data fetch ──────────────────────────────────────────────────────────────

def fetch_twse(code: str, months: int = 3):
    today = datetime.date.today()
    rows = []
    s = requests.Session(); s.headers["User-Agent"] = "Mozilla/5.0"; s.verify = False
    for m in range(months, -1, -1):
        d = (today.replace(day=1) - datetime.timedelta(days=m*30)).replace(day=1)
        ym = d.strftime("%Y%m")
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={ym}01&stockNo={code}"
        try:
            r = s.get(url, timeout=10); data = r.json()
            if data.get("stat") != "OK": continue
            for row in data.get("data", []):
                try:
                    p = row[0].split("/")
                    rows.append({
                        "date":  datetime.date(int(p[0])+1911, int(p[1]), int(p[2])),
                        "Open":  float(row[3].replace(",","")),
                        "High":  float(row[4].replace(",","")),
                        "Low":   float(row[5].replace(",","")),
                        "Close": float(row[6].replace(",","")),
                        "Vol_K": int(row[1].replace(",","")) // 1000,  # → 張
                    })
                except: pass
        except: pass
    rows.sort(key=lambda r: r["date"])
    seen = set(); out = []
    for r in rows:
        if r["date"] not in seen: seen.add(r["date"]); out.append(r)
    return out

# ── Android-equivalent signal check ─────────────────────────────────────────

def _ema(v, span):
    a = 2/(span+1); r = [v[0]]
    for x in v[1:]: r.append(a*x + (1-a)*r[-1])
    return r

def _rsi(arr, p=14):
    if len(arr) < p+1: return 50.0
    d = [arr[i+1]-arr[i] for i in range(len(arr)-1)]; r = d[-p:]
    g = sum(max(x,0) for x in r)/p; l = sum(max(-x,0) for x in r)/p
    return (100-100/(1+g/l)) if l>0 else 100.0

def _bb_std(v):
    m = sum(v)/len(v)
    return math.sqrt(sum((x-m)**2 for x in v)/(len(v)-1)) if len(v)>1 else 0.0

def _kd(hs, ls, cs, p=9):
    n = len(cs); rsv = [0.0]*n
    for i in range(n):
        s = max(0, i-p+1); hi = max(hs[s:i+1]); lo = min(ls[s:i+1])
        rsv[i] = (cs[i]-lo)/(hi-lo)*100 if hi!=lo else 50.0
    a = 1/3; k=[rsv[0]]; d=[rsv[0]]
    for i in range(1,n): k.append(a*rsv[i]+(1-a)*k[-1]); d.append(a*k[-1]+(1-a)*d[-1])
    return k[-1], d[-1], k[-2], d[-2]

def _wr(hs, ls, cs, p=14):
    def w(i):
        s = max(0, i-p+1); hi = max(hs[s:i+1]); lo = min(ls[s:i+1])
        return (hi-cs[i])/(hi-lo)*-100 if hi!=lo else -50.0
    n = len(cs); return w(n-1), w(n-2)

def android_check(bars):
    """
    Simulate Android buildBase(bars) + checkSignals(bars[-1] OHLCV).
    bars[-1] = 訊號日, bars[-2] = 昨日.
    Returns dict of signal flags, or None if filtered.
    """
    n = len(bars)
    if n < 22: return None
    C=[b["Close"] for b in bars]; O=[b["Open"] for b in bars]
    H=[b["High"] for b in bars]; L=[b["Low"] for b in bars]
    V=[b["Vol_K"] for b in bars]  # 張

    # ── buildBase ────────────────────────────────────────────────────────
    avg5 = sum(V[n-6:n-1])/5
    if avg5 < 3000: return None  # MIN_AVG_VOL filter
    avg20 = sum(V[n-21:n-1])/20 if n>=22 else avg5
    vol_today = V[n-1]
    vol_ratio = vol_today/avg5 if avg5>0 else 0
    vol_expand = avg5 > avg20*1.2; vol_shrink = avg5 < avg20*0.8
    tide_shrink = all(V[n-j-1]<V[n-j-2] for j in range(1,4)) if n>=6 else False

    ma5  = sum(C[n-5:])/5; ma10 = sum(C[n-10:])/10; ma20 = sum(C[n-20:])/20
    rsi_now = _rsi(C); rsi_prv = _rsi(C[:-1])
    hi5 = max(H[n-6:n-1]); lo5 = min(L[n-6:n-1])
    three_up = all(C[n-j-1]>O[n-j-1] for j in range(1,4)) if n>=5 else False
    three_dn = all(C[n-j-1]<O[n-j-1] for j in range(1,4)) if n>=5 else False

    prev_c=C[n-2]; prev_o=O[n-2]; prev_body=abs(prev_c-prev_o)
    prev_bull=prev_c>prev_o; prev_bear=prev_c<prev_o

    e12=_ema(C,12); e26=_ema(C,26); ml=[a-b for a,b in zip(e12,e26)]; sl=_ema(ml,9)
    macd_t,macd_p,msig_t,msig_p = ml[-1],ml[-2],sl[-1],sl[-2]

    bb20=C[n-20:]; bm=sum(bb20)/20; bstd=_bb_std(bb20)
    bb_lo_t=bm-2*bstd; bb_up_t=bm+2*bstd
    bb20p=C[n-21:n-1]; bmp=sum(bb20p)/20; bstdp=_bb_std(bb20p)
    bb_lo_p=bmp-2*bstdp; bb_up_p=bmp+2*bstdp

    kk_t,kd_t,kk_p,kd_p = _kd(H,L,C)
    wr_t,wr_p = _wr(H,L,C)

    # ── checkSignals (signal day = bars[-1]) ────────────────────────────
    tc=C[-1]; to=O[-1]; th=H[-1]; tl=L[-1]
    chg = (tc-prev_c)/prev_c*100 if prev_c else 0
    gap = (to-prev_c)/prev_c*100 if prev_c else 0
    body=abs(tc-to); is_bull=tc>to; is_bear=tc<to
    upper=th-max(tc,to); lower=min(tc,to)-tl

    hammer    = body>0 and lower>=2*body and upper<=body and chg<0 and prev_bear
    shoot     = body>0 and upper>=2*body and lower<=body and chg>0 and prev_bull
    ebull     = is_bull and prev_bear and to<=prev_c and tc>=prev_o and body>prev_body*0.8
    ebear     = is_bear and prev_bull and to>=prev_c and tc<=prev_o and body>prev_body*0.8

    d1o=O[-3]; d1c=C[-3]; d1b=abs(d1c-d1o); d1m=(d1c+d1o)/2
    mstar = d1c<d1o and d1b>0 and prev_body<d1b*0.4 and is_bull and tc>d1m and body>=d1b*0.5
    estar = d1c>d1o and d1b>0 and prev_body<d1b*0.4 and is_bear and tc<d1m and body>=d1b*0.5

    s1o=O[-3]; s1c=C[-3]; s2o=O[-2]; s2c=C[-2]
    tsol  = s1c>s1o and s2c>s2o and is_bull and s2c>s1c and tc>s2c and s2o>=s1o and to>=s2o
    tcrow = s1c<s1o and s2c<s2o and is_bear and s2c<s1c and tc<s2c and s2o<=s1o and to<=s2o

    ib2h=H[-3]; ib2l=L[-3]; ib_ins=H[-2]<ib2h and L[-2]>ib2l
    ibq  = ib_ins and tc>ib2h
    ibqs = ib_ins and tc<ib2l and vol_ratio>=1.5

    bias = (tc-ma20)/ma20*100 if ma20 else 0
    ma_bull=ma5>ma10>ma20; ma_bear=ma5<ma10<ma20

    return {
        "A":  ma5>ma20 and chg>0,        "AS": ma5<ma20 and vol_ratio>=1.5 and chg<0,
        "B":  gap>=2 and chg>0,           "BS": gap<=-2 and vol_ratio>=1.3 and chg<0,
        "C":  rsi_prv<35 and rsi_now>rsi_prv and tc>ma5,
        "CS": rsi_prv>65 and rsi_now<rsi_prv and tc<ma5,
        "D":  tc>hi5,                     "DS": tc<lo5 and vol_ratio>=1.5,
        "E":  three_up and ma_bull and chg>0, "ES": three_dn and ma_bear and chg<0,
        "F":  vol_expand and chg>0,       "FS": vol_shrink and vol_ratio>=1.5 and chg<0,
        "G":  tide_shrink and chg>0,      "GS": tide_shrink and vol_ratio>=1.5 and chg<0,
        "H":  hammer,  "HS": shoot,  "I": ebull,  "IS": ebear,
        "J":  macd_p<msig_p and macd_t>msig_t and chg>0,
        "JS": macd_p>msig_p and macd_t<msig_t and chg<0,
        "K":  prev_c<=bb_lo_p and tc>bb_lo_t,
        "KS": prev_c>=bb_up_p and tc<bb_up_t,
        "L":  kk_t<30 and kk_p<kd_p and kk_t>kd_t,
        "LS": kk_t>70 and kk_p>kd_p and kk_t<kd_t,
        "M":  wr_p<-80 and wr_t>wr_p and chg>0,
        "MS": wr_p>-20 and wr_t<wr_p and chg<0,
        "N":  ma_bull and prev_c<ma5 and tc>=ma5,
        "NS": ma_bear and prev_c>ma5 and tc<=ma5,
        "O":  mstar,  "OS": estar,  "P": tsol,  "PS": tcrow,
        "Q":  ibq,    "QS": ibqs,
        "R":  bias<-8 and chg>0,          "RS": bias>8 and chg<0,
        "B2": gap>=5 and vol_ratio>=0.8 and chg>0,
    }

def matched_combos(sigs, preset_name):
    combos = COMBO_PRESETS.get(preset_name, [])
    return ["+".join(c) for c in combos if all(sigs.get(s,False) for s in c)]

# ── Main ────────────────────────────────────────────────────────────────────

def check_code(code):
    bars = fetch_twse(code, months=3)
    today = datetime.date.today()
    bars = [b for b in bars if b["date"] < today]
    if len(bars) < 22:
        print(f"{code}: 資料不足 ({len(bars)} bars)")
        return None
    sigs = android_check(bars)
    if sigs is None:
        print(f"{code}: 均量不足 (avg5 < 3000張)")
        return None

    # Compare with Python
    try:
        import pandas as pd
        from swing_trade_v2 import _check
        df = pd.DataFrame(bars).set_index("date")
        df.index = pd.DatetimeIndex(df.index)
        df = df.rename(columns={"Vol_K":"Vol_K"})
        py_sigs = _check(df, len(df)-1, min_avg_vol=3000, vol_mult=1.5)
        diffs = [k for k in sigs if bool(sigs[k]) != bool(py_sigs.get(k,False))]
        match_str = "✓ 完全對齊" if not diffs else f"✗ 差異: {diffs}"
    except Exception as e:
        py_sigs = {}; match_str = f"(Python比較失敗: {e})"
        diffs = []

    tc = bars[-1]["Close"]; to = bars[-1]["Open"]
    prev_c = bars[-2]["Close"]
    chg = (tc-prev_c)/prev_c*100
    avg5 = sum(b["Vol_K"] for b in bars[-6:-1])/5
    vol_r = bars[-1]["Vol_K"]/avg5

    print(f"\n{'═'*55}")
    print(f"  {code}  收={tc:.2f}  漲跌={chg:+.2f}%  量={bars[-1]['Vol_K']}張  量比={vol_r:.2f}")
    print(f"  訊號日: {bars[-1]['date']}  {match_str}")

    active = [k for k,v in sigs.items() if v]
    print(f"  命中訊號: {active if active else '(無)'}")

    for preset in COMBO_PRESETS:
        hits = matched_combos(sigs, preset)
        if hits:
            print(f"  {preset}: {hits}")
    return sigs

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or "--help" in args:
        print(__doc__); sys.exit()

    if "--scan" in args:
        # Quick scan of major stocks
        from swing_trade_v2 import fetch_data_twse
        import json
        try:
            r = requests.get("https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&selectType=ALL",
                             timeout=10, verify=False, headers={"User-Agent":"Mozilla/5.0"})
            codes = [row[0].strip() for row in r.json().get("data",[])
                     if len(row)>1 and row[0].strip().isdigit() and len(row[0].strip())==4]
        except:
            codes = ["2330","2317","2454","2412","3008","1303","2308","2382"]
        print(f"掃描 {len(codes)} 支股票...")
        data = fetch_data_twse(codes[:200], days=70)
        today = datetime.date.today()
        results = []
        for code, df in data.items():
            bars = []
            for _, row in df.reset_index().iterrows():
                d = row.iloc[0].date() if hasattr(row.iloc[0],"date") else row["Date"].date()
                if d >= today: continue
                bars.append({"date":d,"Open":float(row["Open"]),"High":float(row["High"]),
                             "Low":float(row["Low"]),"Close":float(row["Close"]),"Vol_K":float(row["Vol_K"])})
            if len(bars) < 22: continue
            sigs = android_check(bars)
            if sigs is None: continue
            hits = {p: matched_combos(sigs, p) for p in COMBO_PRESETS}
            total = sum(len(v) for v in hits.values())
            if total > 0:
                results.append((code, total, bars[-1]["Close"], bars[-1]["Vol_K"], hits))
        results.sort(key=lambda x: -x[1])
        print(f"\n找到 {len(results)} 支有訊號的股票：")
        for code, total, close, vol, hits in results[:50]:
            combo_str = " | ".join(f"{p}:{v}" for p,v in hits.items() if v)
            print(f"  {code}  收={close:.2f}  量={vol}張  {combo_str}")
    else:
        for code in args:
            check_code(code.strip())
