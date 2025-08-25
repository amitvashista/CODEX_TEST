
from __future__ import annotations
import csv
def _split_aliases(s: str | None) -> list[str]:
    return [] if not s else [a.strip() for a in s.split(";") if a.strip()]
def load_symbol_index(csv_path: str) -> list[dict]:
    out = []
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sym = (row.get("symbol") or "").strip().upper()
            name = (row.get("company_name") or "").strip()
            aliases = _split_aliases(row.get("aliases"))
            if not sym or not name: continue
            aliases_full = [name] + aliases + [sym]
            out.append({"symbol": sym, "name": name, "aliases": aliases_full, "aliases_upper": [x.upper() for x in aliases_full]})
    return out
def map_symbols(text: str, index: list[dict], max_symbols: int = 5) -> list[str]:
    if not text: return []
    T = text.upper()
    hits = []
    for rec in index:
        if any(alias and alias in T for alias in rec["aliases_upper"]):
            hits.append(rec["symbol"])
            if len(hits) >= max_symbols: break
    uniq = []
    seen = set()
    for h in hits:
        if h not in seen:
            seen.add(h); uniq.append(h)
    return uniq
