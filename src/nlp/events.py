
from __future__ import annotations
import re
EVENT_PATTERNS = {
    "EARNINGS": [re.compile(r"(?i)results? (?:for|of|q[1-4]|quarter|annual|fy\d{2,4})"),
                 re.compile(r"(?i)financial (?:results|statements)")],
    "DIVIDEND": [re.compile(r"(?i)dividend"), re.compile(r"(?i)interim dividend|final dividend")],
    "BUYBACK": [re.compile(r"(?i)buy\s*back|share repurchase")],
    "SPLIT": [re.compile(r"(?i)stock split|split.*face value")],
    "BONUS": [re.compile(r"(?i)bonus (?:issue|shares)")],
    "MERGER_ACQUISITION": [re.compile(r"(?i)merger|amalgamation|acquisition|acquires|takeover")],
    "BOARD_MEETING": [re.compile(r"(?i)board meeting")],
    "PLEDGE": [re.compile(r"(?i)pledge|pledging of shares")],
    "LITIGATION": [re.compile(r"(?i)litigation|lawsuit|legal notice|court order|writ")],
    "REGULATORY": [re.compile(r"(?i)SEBI|RBI|NCLT|NCLAT|SAT|regulator|show cause")],
    "RATING_ACTION": [re.compile(r"(?i)credit rating|rating (?:upgrade|downgrade|affirmed)"),
                      re.compile(r"(?i)CRISIL|ICRA|CARE Ratings|Fitch|Moody")],
    "ORDER_WIN": [re.compile(r"(?i)order win|secures order|bags order|contract worth")],
    "GUIDANCE": [re.compile(r"(?i)guidance|outlook|revenue guidance|EBITDA guidance")],
    "CAPEX": [re.compile(r"(?i)capex|capital expenditure|expansion plan")],
    "INSIDER_TRADE": [re.compile(r"(?i)insider trading|promoter (?:buy|sell)|share sale by promoter")],
    "FUNDRAISE": [re.compile(r"(?i)QIP|qualified institutional placement|rights issue|preferential issue|NCD|debenture issue")],
}
def detect_events(text: str) -> list[str]:
    tags = []
    for tag, patterns in EVENT_PATTERNS.items():
        if any(p.search(text or "") for p in patterns):
            tags.append(tag)
    return tags
