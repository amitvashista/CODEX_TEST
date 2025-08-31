from __future__ import annotations
from sqlalchemy import create_engine, String, Text, Float, Integer
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, Session
import pandas as pd

BaseFE = declarative_base()

class Features(BaseFE):
    __tablename__ = "features"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    fe_date: Mapped[str] = mapped_column(String(16), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    news_count: Mapped[int] = mapped_column(Integer, nullable=True)
    is_pos_sum: Mapped[int] = mapped_column(Integer, nullable=True)
    is_neg_sum: Mapped[int] = mapped_column(Integer, nullable=True)
    is_neu_sum: Mapped[int] = mapped_column(Integer, nullable=True)
    sent_mean: Mapped[str] = mapped_column(String(32), nullable=True)
    sent_max: Mapped[str] = mapped_column(String(32), nullable=True)
    sent_min: Mapped[str] = mapped_column(String(32), nullable=True)
    pos_ratio: Mapped[str] = mapped_column(String(32), nullable=True)
    neg_ratio: Mapped[str] = mapped_column(String(32), nullable=True)
    EARNINGS: Mapped[int] = mapped_column(Integer, nullable=True)
    DIVIDEND: Mapped[int] = mapped_column(Integer, nullable=True)
    ORDER_WIN: Mapped[int] = mapped_column(Integer, nullable=True)
    close: Mapped[float] = mapped_column(Float, nullable=True)
    sma_20: Mapped[float] = mapped_column(Float, nullable=True)
    ema_20: Mapped[float] = mapped_column(Float, nullable=True)
    ema_50: Mapped[float] = mapped_column(Float, nullable=True)
    rsi: Mapped[float] = mapped_column(Float, nullable=True)
    macd: Mapped[float] = mapped_column(Float, nullable=True)
    macd_signal: Mapped[float] = mapped_column(Float, nullable=True)
    macd_hist: Mapped[float] = mapped_column(Float, nullable=True)
    atr: Mapped[float] = mapped_column(Float, nullable=True)
    vol_20: Mapped[float] = mapped_column(Float, nullable=True)

class FeaturesDB:
    def __init__(self, path: str):
        self.engine = create_engine(f"sqlite:///{path}", future=True)

    def create_tables(self) -> None:
        BaseFE.metadata.create_all(self.engine)

    def insert_many(self, df: pd.DataFrame) -> int:
        if df is None or df.empty: return 0
        rows = df.to_dict(orient="records")
        with Session(self.engine) as s:
            for r in rows:
                s.execute(
                    Features.__table__.delete().where(
                        (Features.fe_date == r.get("fe_date")) & (Features.symbol == r.get("symbol"))
                    )
                )
                obj = Features(
                    fe_date=r.get("fe_date"),
                    symbol=r.get("symbol"),
                    news_count=int(r.get("news_count") or 0),
                    is_pos_sum=int(r.get("is_pos_sum") or 0),
                    is_neg_sum=int(r.get("is_neg_sum") or 0),
                    is_neu_sum=int(r.get("is_neu_sum") or 0),
                    sent_mean=str(r.get("sent_mean")) if r.get("sent_mean") is not None else None,
                    sent_max=str(r.get("sent_max")) if r.get("sent_max") is not None else None,
                    sent_min=str(r.get("sent_min")) if r.get("sent_min") is not None else None,
                    pos_ratio=str(r.get("pos_ratio")) if r.get("pos_ratio") is not None else None,
                    neg_ratio=str(r.get("neg_ratio")) if r.get("neg_ratio") is not None else None,
                    EARNINGS=int(r.get("EARNINGS") or 0) if "EARNINGS" in r else None,
                    DIVIDEND=int(r.get("DIVIDEND") or 0) if "DIVIDEND" in r else None,
                    ORDER_WIN=int(r.get("ORDER_WIN") or 0) if "ORDER_WIN" in r else None,
                    close=float(r.get("close")) if r.get("close") is not None else None,
                    sma_20=float(r.get("sma_20")) if r.get("sma_20") is not None else None,
                    ema_20=float(r.get("ema_20")) if r.get("ema_20") is not None else None,
                    ema_50=float(r.get("ema_50")) if r.get("ema_50") is not None else None,
                    rsi=float(r.get("rsi")) if r.get("rsi") is not None else None,
                    macd=float(r.get("macd")) if r.get("macd") is not None else None,
                    macd_signal=float(r.get("macd_signal")) if r.get("macd_signal") is not None else None,
                    macd_hist=float(r.get("macd_hist")) if r.get("macd_hist") is not None else None,
                    atr=float(r.get("atr")) if r.get("atr") is not None else None,
                    vol_20=float(r.get("vol_20")) if r.get("vol_20") is not None else None,
                )
                s.add(obj)
            s.commit()
        return len(rows)
