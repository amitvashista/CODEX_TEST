
from __future__ import annotations
from typing import Dict, Tuple
_POS_WORDS = {"profit","growth","surge","rally","upgrade","order win","bags order","raises guidance","beat","beats","dividend","bonus","buyback","record","approval","approved","secures","margin expansion","expansion","qip success","all-time high","acquires"}
_NEG_WORDS = {"loss","decline","falls","downgrade","probe","fraud","pledge","default","delay","resigns","resignation","litigation","penalty","raid","sebi notice","weak","guidance cut","miss","fire","closure","strike","bankruptcy","insolvency"}
class SentimentEngine:
    def __init__(self, engine: str = "rule", hf_model: str = "ProsusAI/finbert"):
        self.engine = engine
        self.hf_model = hf_model
        self._pipe = None
        if engine == "hf_finbert":
            try:
                from transformers import pipeline
                self._pipe = pipeline("text-classification", model=hf_model, truncation=True)
            except Exception:
                self.engine = "rule"
                self._pipe = None
    def _rule_score(self, text: str) -> tuple[str, float]:
        t = (text or "").lower()
        pos = sum(1 for w in _POS_WORDS if w in t)
        neg = sum(1 for w in _NEG_WORDS if w in t)
        if pos == 0 and neg == 0: return "neutral", 0.0
        score = (pos - neg) / max(1, pos + neg)
        if score > 0.15: return "positive", float(score)
        if score < -0.15: return "negative", float(score)
        return "neutral", float(score)
    def score(self, text: str) -> Dict:
        if self.engine == "hf_finbert" and self._pipe is not None:
            try:
                out = self._pipe(text[:512])[0]
                label = out.get("label","neutral").lower()
                score = float(out.get("score", 0.0))
                signed = score if "pos" in label else (-score if "neg" in label else 0.0)
                return {"label": label, "score": signed, "engine": "hf_finbert"}
            except Exception:
                pass
        label, s = self._rule_score(text)
        return {"label": label, "score": s, "engine": "rule"}
