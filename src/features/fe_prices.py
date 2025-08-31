from __future__ import annotations
from typing import List, Dict
import pandas as pd
import yfinance as yf
from .indicators import sma, ema, rsi, macd, atr, realized_vol

def fetch_prices(symbols: List[str], lookback_days: int, end_date: str) -> Dict[str, pd.DataFrame]:
    prices = {}
    end = pd.to_datetime(end_date)
    start = end - pd.Timedelta(days=lookback_days + 10)
    for s in symbols:
        ysym = s if s.endswith(".NS") else f"{s}.NS"
        try:
            df = yf.download(ysym, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), progress=False)
            if df.empty:
                continue
            df = df.rename(columns=str.lower).reset_index().rename(columns={"index":"date"})
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
            prices[s] = df
        except Exception:
            continue
    return prices

def compute_indicators(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    p = cfg.get("indicators", {})
    rsi_p = int(p.get("rsi_period", 14))
    macd_f = int(p.get("macd_fast", 12))
    macd_s = int(p.get("macd_slow", 26))
    macd_sig = int(p.get("macd_signal", 9))
    atr_p = int(p.get("atr_period", 14))

    out = df.copy()
    out["sma_20"] = sma(out["close"], 20)
    out["ema_20"] = ema(out["close"], 20)
    out["ema_50"] = ema(out["close"], 50)
    out["rsi"] = rsi(out["close"], rsi_p)
    macd_line, sig_line, hist = macd(out["close"], macd_f, macd_s, macd_sig)
    out["macd"] = macd_line; out["macd_signal"] = sig_line; out["macd_hist"] = hist
    out["atr"] = atr(out["high"], out["low"], out["close"], atr_p)
    out["ret_1d"] = out["close"].pct_change(1)
    out["ret_5d"] = out["close"].pct_change(5)
    out["vol_20"] = realized_vol(out["close"], 20)
    out["vol_chg"] = out["volume"].pct_change(1)
    return out

def latest_asof(df: pd.DataFrame, date_str: str):
    d = pd.to_datetime(date_str)
    f = df[df["date"] <= d]
    if f.empty:
        return None
    return f.sort_values("date").iloc[-1]
