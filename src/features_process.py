from __future__ import annotations
import argparse, json
from pathlib import Path
import yaml, pandas as pd
from src.utils.logger import get_logger
from src.features.fe_news import build_news_features
from src.features.fe_prices import fetch_prices, compute_indicators, latest_asof
from src.storage.db_features_addon import FeaturesDB

def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser(description="Phase 2: Feature Engineering (news + prices)")
    ap.add_argument("--date", type=str, default="today", help="YYYY-MM-DD or 'today' (local)")
    ap.add_argument("--config", type=str, default="config/config.yaml")
    ap.add_argument("--lookback", type=int, default=None, help="Override lookback days")
    args = ap.parse_args()

    logger = get_logger(__name__)
    cfg = load_config(args.config)
    feat_cfg = cfg.get("features", {})
    lookback = args.lookback or int(feat_cfg.get("lookback_days", 180))

    raw_day = args.date
    if raw_day == "today":
        from datetime import datetime
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(cfg.get("timezone","Asia/Kolkata"))
        raw_day = datetime.now(tz).date().isoformat()

    proc_path = Path(f"data/processed/{raw_day}.json")
    if not proc_path.exists():
        logger.error(f"Processed NLP file not found: {proc_path}. Run src/nlp_process.py first.")
        return

    nlp_rows = json.loads(proc_path.read_text(encoding="utf-8"))
    news_df = build_news_features(nlp_rows)
    if news_df.empty:
        logger.warning("No news features to build."); return

    symbols = sorted(news_df["symbol"].unique().tolist())
    logger.info(f"Symbols to fetch prices for: {len(symbols)}")
    prices = fetch_prices(symbols, lookback_days=lookback, end_date=raw_day)

    price_rows = []
    for sym, df in prices.items():
        if df is None or df.empty: continue
        df2 = compute_indicators(df, cfg.get("features", {}))
        row = latest_asof(df2, raw_day)
        if row is not None:
            r = row.to_dict(); r["symbol"] = sym; price_rows.append(r)

    price_df = pd.DataFrame(price_rows)
    if price_df.empty:
        logger.error("No price rows computed."); return

    merged = news_df.merge(price_df, on="symbol", how="left", suffixes=("_news",""))
    merged["fe_date"] = raw_day

    out_dir = Path("data/processed/features"); out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{raw_day}.csv"
    merged.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"Wrote features: {out_path} with {len(merged)} rows")

    db = FeaturesDB("db/news.db"); db.create_tables(); ins = db.insert_many(merged)
    logger.info(f"Inserted/updated {ins} feature rows into DB")

if __name__ == "__main__":
    main()
