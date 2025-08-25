
from __future__ import annotations
import argparse, json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import yaml, pandas as pd
from src.utils.logger import get_logger
from src.nlp.clean import clean_text
from src.nlp.events import detect_events
from src.nlp.sentiment import SentimentEngine
from src.nlp.ticker_map import load_symbol_index, map_symbols
from src.storage.db_nlp_addon import NewsDB_NLP
def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
def main():
    ap = argparse.ArgumentParser(description="Phase 2: NLP process for daily news")
    ap.add_argument("--date", type=str, default="today", help="YYYY-MM-DD (IST) or 'today'")
    ap.add_argument("--config", type=str, default="config/config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    tz = ZoneInfo(cfg.get("timezone", "Asia/Kolkata"))
    run_day = (datetime.now(tz).date().isoformat() if args.date == "today" else datetime.fromisoformat(args.date).date().isoformat())
    logger = get_logger(__name__); logger.info(f"Phase 2 NLP for {run_day}")
    raw_file = Path(f"data/raw/{run_day}.json")
    if not raw_file.exists():
        logger.error(f"Raw file not found: {raw_file}. Run Phase 1 first."); return
    sym_csv = cfg.get("reference", {}).get("nse_symbols_csv", "data/reference/nse_symbols.csv")
    index = load_symbol_index(sym_csv)
    sent_cfg = cfg.get("nlp", {}).get("sentiment", {})
    engine = SentimentEngine(engine=sent_cfg.get("engine","rule"), hf_model=sent_cfg.get("hf_model","ProsusAI/finbert"))
    items = json.loads(raw_file.read_text(encoding="utf-8"))
    out_rows = []
    for it in items:
        title = it.get("title") or ""; summary = it.get("summary") or ""
        text = clean_text(f"{title}. {summary}")
        events = detect_events(text) if cfg.get("nlp", {}).get("events", {}).get("enabled", True) else []
        symbols_existing = it.get("company_symbols") or []
        symbols_detected = map_symbols(text, index, max_symbols=cfg.get("nlp", {}).get("ticker_map", {}).get("max_symbols", 5))
        symbols = list(dict.fromkeys([*symbols_existing, *symbols_detected]))
        sent = engine.score(text)
        out_rows.append({
            "url": it.get("url",""), "title": title, "published_at": it.get("published_at"),
            "symbols": symbols, "events": events,
            "sentiment_label": sent["label"], "sentiment_score": sent["score"], "sentiment_engine": sent["engine"],
            "source": it.get("source","rss"),
        })
    out_dir = Path("data/processed"); out_dir.mkdir(parents=True, exist_ok=True)
    jp = out_dir / f"{run_day}.json"; cp = out_dir / f"{run_day}.csv"
    jp.write_text(json.dumps(out_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(out_rows).to_csv(cp, index=False, encoding="utf-8")
    db = NewsDB_NLP("db/news.db"); db.create_tables(); ins = db.insert_many(out_rows)
    logger.info(f"Processed items: {len(out_rows)} | Inserted into DB: {ins}"); logger.info(f"Wrote: {jp} and {cp}")
if __name__ == "__main__":
    main()
