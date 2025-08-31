from __future__ import annotations
from typing import Dict, List
import pandas as pd

EVENTS = [
    "EARNINGS","DIVIDEND","BUYBACK","SPLIT","BONUS","MERGER_ACQUISITION","BOARD_MEETING",
    "PLEDGE","LITIGATION","REGULATORY","RATING_ACTION","ORDER_WIN","GUIDANCE","CAPEX",
    "INSIDER_TRADE","FUNDRAISE"
]

def _explode_rows(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows:
        syms = r.get("symbols") or []
        for s in syms:
            out.append({**r, "symbol": s})
    return out

def build_news_features(nlp_rows: List[Dict]) -> pd.DataFrame:
    exploded = _explode_rows(nlp_rows)
    if not exploded:
        return pd.DataFrame(columns=["date","symbol"])
    df = pd.DataFrame(exploded)
    df["date"] = pd.to_datetime(df["published_at"].str[:10], errors="coerce")
    df = df.dropna(subset=["date","symbol"])
    df["is_pos"] = (df["sentiment_label"] == "positive").astype(int)
    df["is_neg"] = (df["sentiment_label"] == "negative").astype(int)
    df["is_neu"] = (df["sentiment_label"] == "neutral").astype(int)
    df["s_score"] = df["sentiment_score"].astype(float)
    for e in EVENTS:
        df[e] = df["events"].apply(lambda ev: int(e in (ev or [])))
    agg = {
        "url": "count",
        "is_pos": "sum",
        "is_neg": "sum",
        "is_neu": "sum",
        "s_score": ["mean","max","min"],
    }
    for e in EVENTS:
        agg[e] = "sum"
    g = df.groupby(["date","symbol"]).agg(agg)
    g.columns = ["_".join([c for c in col if c]).strip("_") for col in g.columns.values]
    g = g.rename(columns={"url_count":"news_count","s_score_mean":"sent_mean","s_score_max":"sent_max","s_score_min":"sent_min"}).reset_index()
    g["pos_ratio"] = g["is_pos_sum"] / g["news_count"].clip(lower=1)
    g["neg_ratio"] = g["is_neg_sum"] / g["news_count"].clip(lower=1)
    return g
