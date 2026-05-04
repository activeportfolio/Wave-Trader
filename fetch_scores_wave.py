"""
fetch_scores_wave.py
Wave Trader — Surf Report
Fetches 1h bars from Twelve Data, aggregates to 195-min (3 × 1h),
computes Swell Score (BBW/ATR) + Takeoff Meter (10-pt system).
"""

import os, json, time, math, requests, datetime
from datetime import datetime as dt, timezone

TD_KEY = os.environ["TD_KEY"]
BASE   = "https://api.twelvedata.com"
BATCH  = 8
DELAY  = 2.5

TICKERS = [
    "AAL","AAOI","AAPL","ABT","ACHR","ADBE","AG","AGNC","AI","ALAB",
    "AMAT","AMC","AMD","AMPX","AMZN","ANET","APLD","APP","ARM","ASTS",
    "AVGO","BA","BABA","BAC","BB","BBAI","BE","BMNR","BMY","BSX",
    "BTDR","BULL","C","CAT","CCL","CELH","CIFR","CLF","CLOV","CLSK",
    "CMCSA","CMG","CNC","COIN","CORZ","COST","CRCL","CRDO","CRM","CRML",
    "CRWD","CRWV","CSCO","CVNA","CVS","CVX","DAL","DDOG","DELL","DIS",
    "DJT","DKNG","DOW","DVN","ENPH","EOSE","EPD","ET","F","FCX",
    "FIG","FRMI","FSLR","GE","GEV","GLW","GLXY","GM","GME","GOOG",
    "GOOGL","GRAB","GS","HAL","HIMS","HL","HOOD","HPE","HPQ","HTZ",
    "IBM","IBRX","INTC","IONQ","IREN","JBLU","JD","JNJ","JOBY","JPM",
    "KHC","KO","LAC","LCID","LITE","LLY","LRCX","LULU","LUMN","LUNR",
    "LYFT","MARA","MCD","META","MP","MRK","MRNA","MRVL","MSFT","MSTR",
    "MU","NBIS","NCLH","NEM","NIO","NKE","NN","NOK","NOW","NU",
    "NVDA","NVO","NVTS","OGN","OKLO","ON","ONDS","OPEN","ORCL","OSCR",
    "OWL","OXY","PAA","PAGP","PANW","PATH","PBR","PCG","PDD","PFE",
    "PG","PLTR","PLUG","PTON","PYPL","QBTS","QCOM","QS","QUBT","QXO",
    "RBLX","RCAT","RDDT","RDW","RGTI","RIG","RIOT","RIVN","RKLB","RKT",
    "RTX","SBET","SBUX","SCHW","SHOP","SIRI","SLB","SMCI","SMR","SNAP",
    "SNDK","SNOW","SOFI","SOUN","STM","STX","T","TEAM","TGT","TLRY",
    "TSLA","TSM","TTD","TXN","U","UAL","UAMY","UBER","UNH","UPS",
    "USAR","UUUU","V","VALE","VG","VRT","VST","VZ","WBD","WDC",
    "WFC","WMT","WULF","XOM","XPEV","XYZ","ZETA","ZM",
]

# ── 1h → 195-min aggregation ──────────────────────────────────────────────────
def aggregate_195min(bars_1h):
    """Convert newest-first 1h bars to oldest-first 195-min bars (groups of 3)."""
    bars = list(reversed(bars_1h))
    result = []
    for i in range(0, len(bars) - 2, 3):
        g = bars[i : i + 3]
        if len(g) < 3:
            break
        result.append({
            "open":  float(g[0]["open"]),
            "high":  max(float(b["high"]) for b in g),
            "low":   min(float(b["low"])  for b in g),
            "close": float(g[-1]["close"]),
        })
    return result  # oldest-first

# ── Indicators ────────────────────────────────────────────────────────────────
def calc_bbw(closes, period=20, lookback=252):
    arr = []
    for i in range(period - 1, len(closes)):
        sl = closes[i - period + 1 : i + 1]
        m  = sum(sl) / period
        sd = math.sqrt(sum((v - m) ** 2 for v in sl) / period)
        arr.append((4 * sd) / m if m else 0)
    if not arr:
        return None
    cur    = arr[-1]
    window = arr[-lookback:]
    mn     = min(window)
    return {"currentBBW": cur, "minBBW": mn, "relativeBBW": cur / mn if mn else 0}


def calc_atr(highs, lows, closes, period=14):
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr


def calc_rsi(closes, period=14):
    ch = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    if len(ch) < period:
        return None, None
    ag = sum(c for c in ch[:period] if c > 0) / period
    al = sum(-c for c in ch[:period] if c < 0) / period
    rsis = []
    for i in range(period, len(ch)):
        ag = (ag * (period-1) + max(ch[i], 0)) / period
        al = (al * (period-1) + max(-ch[i], 0)) / period
        rsis.append(100 if al == 0 else 100 - (100 / (1 + ag/al)))
    if not rsis:
        return None, None
    desc = list(reversed(rsis))
    last_signal = None
    for i in range(len(desc) - 1):
        if desc[i] >= 70 and desc[i+1] < 70:
            last_signal = {"signal": "OB", "barsAgo": i}; break
        if desc[i] <= 30 and desc[i+1] > 30:
            last_signal = {"signal": "OS", "barsAgo": i}; break
    return rsis[-1], last_signal

# ── Swell Score ───────────────────────────────────────────────────────────────
def calc_swell_score(bars195, lookback=252):
    if len(bars195) < 50:
        raise ValueError("insufficient data")
    closes = [b["close"] for b in bars195]
    highs  = [b["high"]  for b in bars195]
    lows   = [b["low"]   for b in bars195]
    price  = closes[-1]
    bb     = calc_bbw(closes, 20, lookback)
    atr    = calc_atr(highs, lows, closes, 14)
    if not bb or not atr:
        raise ValueError("calc failed")
    nm    = bb["currentBBW"] / (atr / price)
    score = 100 - (bb["relativeBBW"] * nm)
    return {"score": round(score, 2), "price": round(price, 2)}

# ── Takeoff Meter (10 pts) ────────────────────────────────────────────────────
def calc_takeoff_meter(bars195):
    """
    10-point system:
      1 pt  — 5-day MA slope (10 bars at 2 bars/day positive)
      1 pt  — price above 5-day MA
      3 pts — # of trailing 3 ROC bars that are positive (3-bar ROC)
      5 pts — current ROC proximity to 90th percentile (20-bar lookback)
    """
    closes = [b["close"] for b in bars195]
    n = len(closes)
    if n < 25:
        return {"score": 0, "breakdown": {}}

    score = 0
    bd    = {}

    # 1 & 2 — 5-day MA (10 bars)
    ma_period = 10
    ma_arr = [
        sum(closes[i - ma_period + 1 : i + 1]) / ma_period
        for i in range(ma_period - 1, n)
    ]
    ma_slope    = 1 if len(ma_arr) >= 2 and ma_arr[-1] > ma_arr[-2] else 0
    price_vs_ma = 1 if closes[-1] > ma_arr[-1] else 0
    score += ma_slope + price_vs_ma
    bd["maSlope"]    = ma_slope
    bd["priceVsMa"]  = price_vs_ma

    # 3 — Trailing 3 ROC bars (3-bar ROC)
    roc_len  = 3
    roc_bars = 0
    for i in range(3):
        idx = n - 1 - i
        if idx >= roc_len:
            roc = (closes[idx] - closes[idx - roc_len]) / closes[idx - roc_len] * 100
            if roc > 0:
                roc_bars += 1
    score += roc_bars
    bd["rocBars"] = roc_bars

    # 4 — ROC proximity to 90th percentile (20-bar lookback)
    pct_len    = 20
    start      = max(roc_len, n - pct_len)
    roc_values = [
        (closes[i] - closes[i - roc_len]) / closes[i - roc_len] * 100
        for i in range(start, n)
        if i >= roc_len
    ]

    roc_pct = 1
    if roc_values and n > roc_len:
        sorted_roc = sorted(roc_values)
        p90_idx    = max(0, math.ceil(0.9 * len(sorted_roc)) - 1)
        p90        = sorted_roc[p90_idx]
        cur_roc    = (closes[-1] - closes[-1 - roc_len]) / closes[-1 - roc_len] * 100

        if p90 > 0:
            if cur_roc >= p90:
                roc_pct = 5
            else:
                pct_below = (p90 - cur_roc) / abs(p90)
                if pct_below   <= 0.10: roc_pct = 4
                elif pct_below <= 0.20: roc_pct = 3
                elif pct_below <= 0.30: roc_pct = 2
                else:                   roc_pct = 1
        else:
            roc_pct = 3 if cur_roc >= p90 else 1

    score += roc_pct
    bd["rocPercentile"] = roc_pct

    return {"score": min(10, max(0, score)), "breakdown": bd}

# ── API ───────────────────────────────────────────────────────────────────────
def td_fetch(url, retries=2):
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            d = r.json()
            if d.get("status") == "error":
                msg = d.get("message", "API error")
                if "limit" in msg.lower():
                    wait = 6 * (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s…")
                    time.sleep(wait)
                    continue
                raise ValueError(msg)
            return d
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(3 * (attempt + 1))


def norm_batch(data, syms):
    return {syms[0]: data} if len(syms) == 1 else data


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    scores = {}
    errors = {}
    total  = len(TICKERS)

    for i, batch in enumerate(chunks(TICKERS, BATCH)):
        sym_str = ",".join(batch)
        print(f"Batch {i+1}/{math.ceil(total/BATCH)}: {sym_str}")

        try:
            # outputsize=1000 → ~333 195-min bars after aggregation (enough for 252 lookback)
            raw = td_fetch(
                f"{BASE}/time_series?symbol={sym_str}&interval=1h&outputsize=1000&apikey={TD_KEY}"
            )
        except Exception as e:
            print(f"  Batch failed: {e}")
            for s in batch:
                errors[s] = str(e)
            time.sleep(DELAY)
            continue

        data = norm_batch(raw, batch)

        for sym in batch:
            try:
                d = data.get(sym, {})
                if d.get("status") == "error":
                    raise ValueError(d.get("message", "error"))

                values = d.get("values", [])
                if len(values) < 90:
                    raise ValueError(f"only {len(values)} 1h bars")

                bars195 = aggregate_195min(values)
                if len(bars195) < 50:
                    raise ValueError(f"only {len(bars195)} 195-min bars")

                swell   = calc_swell_score(bars195, 252)
                takeoff = calc_takeoff_meter(bars195)

                closes = [b["close"] for b in bars195]
                rsi, last_signal = calc_rsi(closes, 14)

                scores[sym] = {
                    "swellScore":       swell["score"],
                    "price":            swell["price"],
                    "takeoffMeter":     takeoff["score"],
                    "takeoffBreakdown": takeoff["breakdown"],
                    "rsi":              round(rsi, 2) if rsi is not None else None,
                    "lastSignal":       last_signal,
                }
                rsi_str = f"{rsi:.1f}" if rsi is not None else "—"
                print(f"  {sym}: Swell={swell['score']:.1f}  Takeoff={takeoff['score']}/10  RSI={rsi_str}")

            except Exception as e:
                print(f"  {sym} error: {e}")
                errors[sym] = str(e)

        time.sleep(DELAY)

    # Earnings via Yahoo Finance
    earnings = {}
    print("\nFetching earnings from Yahoo Finance…")
    today = datetime.date.today()
    for sym in TICKERS:
        try:
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules=calendarEvents"
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if not r.ok:
                continue
            d = r.json()
            dates = (d.get("quoteSummary") or {}).get("result", [{}])[0].get("calendarEvents", {}).get("earnings", {}).get("earningsDate", [])
            if not dates:
                continue
            candidates = []
            for e in dates:
                raw = e.get("raw")
                if raw is None:
                    continue
                ed   = datetime.date.fromtimestamp(raw)
                days = (ed - today).days
                candidates.append({"date": str(ed), "days": days})
            if candidates:
                upcoming = [c for c in candidates if c["days"] >= 0]
                pick = min(upcoming, key=lambda x: x["days"]) if upcoming else min(candidates, key=lambda x: abs(x["days"]))
                earnings[sym] = pick["date"]
        except Exception:
            pass
        time.sleep(0.1)
    print(f"  Earnings loaded for {len(earnings)} tickers")

    output = {
        "updated":  dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":    len(scores),
        "scores":   scores,
        "earnings": earnings,
        "errors":   errors,
    }

    with open("scores.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"\nDone — {len(scores)} scores, {len(errors)} errors.")
    if errors:
        print("Errors:", list(errors.keys()))


if __name__ == "__main__":
    main()
