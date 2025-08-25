
from src.nlp.events import detect_events
def test_detect_events_basic():
    t = "Company secures order worth 500 crore and announces interim dividend."
    tags = detect_events(t)
    assert "ORDER_WIN" in tags and "DIVIDEND" in tags
