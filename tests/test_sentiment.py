
from src.nlp.sentiment import SentimentEngine
def test_rule_sentiment_positive():
    se = SentimentEngine(engine="rule")
    out = se.score("Company reports profit growth and order win; raises guidance.")
    assert out["label"] == "positive"
def test_rule_sentiment_negative():
    se = SentimentEngine(engine="rule")
    out = se.score("Company faces SEBI probe and promoter resigns after default.")
    assert out["label"] == "negative"
