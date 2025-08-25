
from __future__ import annotations
import re, html
_whitespace_re = re.compile(r"\s+")
_url_re = re.compile(r"https?://\S+")
_html_tag_re = re.compile(r"<[^>]+>")
def clean_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = _html_tag_re.sub(" ", text)
    text = _url_re.sub(" ", text)
    text = text.replace("\u00a0", " ").replace("\xa0", " ")
    return _whitespace_re.sub(" ", text).strip()
