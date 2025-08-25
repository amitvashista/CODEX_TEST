
from __future__ import annotations
from typing import List, Dict
from sqlalchemy import create_engine, String, Text, select
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, Session
BaseNLP = declarative_base()
class NewsNLP(BaseNLP):
    __tablename__ = "news_nlp"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    title: Mapped[str] = mapped_column(Text, nullable=True)
    published_at: Mapped[str] = mapped_column(String(32), nullable=True)
    symbols: Mapped[str] = mapped_column(Text, nullable=True)
    events: Mapped[str] = mapped_column(Text, nullable=True)
    sentiment_label: Mapped[str] = mapped_column(String(16), nullable=True)
    sentiment_score: Mapped[str] = mapped_column(String(32), nullable=True)
    sentiment_engine: Mapped[str] = mapped_column(String(16), nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=True)
class NewsDB_NLP:
    def __init__(self, path: str):
        self.engine = create_engine(f"sqlite:///{path}", future=True)
    def create_tables(self) -> None:
        BaseNLP.metadata.create_all(self.engine)
    def insert_many(self, rows: List[Dict]) -> int:
        if not rows: return 0
        inserted = 0
        with Session(self.engine) as s:
            for r in rows:
                exists = s.execute(select(NewsNLP).where(NewsNLP.url == r.get("url"))).scalar_one_or_none()
                if exists:
                    exists.symbols = ",".join(r.get("symbols") or [])
                    exists.events = ",".join(r.get("events") or [])
                    exists.sentiment_label = r.get("sentiment_label")
                    exists.sentiment_score = str(r.get("sentiment_score"))
                    exists.sentiment_engine = r.get("sentiment_engine")
                    s.add(exists)
                else:
                    row = NewsNLP(
                        url=r.get("url",""), title=r.get("title"),
                        published_at=r.get("published_at"),
                        symbols=",".join(r.get("symbols") or []),
                        events=",".join(r.get("events") or []),
                        sentiment_label=r.get("sentiment_label"),
                        sentiment_score=str(r.get("sentiment_score")),
                        sentiment_engine=r.get("sentiment_engine"),
                        source=r.get("source"),
                    ); s.add(row); inserted += 1
            s.commit()
        return inserted
