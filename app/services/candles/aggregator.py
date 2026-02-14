from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from app.services.market.models import Candle, SpotTick


@dataclass(slots=True)
class _WorkingCandle:
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float


class CandleAggregator:
    def __init__(self, timeframe_seconds: int) -> None:
        if timeframe_seconds <= 0:
            raise ValueError("timeframe_seconds must be > 0")
        self._tf = timeframe_seconds
        self._working: Optional[_WorkingCandle] = None

    def _bucket_start(self, ts: datetime) -> datetime:
        if ts.tzinfo is None:
            raise ValueError("tick timestamp must be timezone-aware")
        epoch = int(ts.timestamp())
        bucket_epoch = epoch - (epoch % self._tf)
        return datetime.fromtimestamp(bucket_epoch, tz=ts.tzinfo)

    def push(self, tick: SpotTick) -> Optional[Candle]:
        """
        Push one tick. Returns a completed candle when a new bucket starts,
        otherwise returns None.
        """
        start = self._bucket_start(tick.ts)
        end = start + timedelta(seconds=self._tf)

        w = self._working
        if w is None:
            self._working = _WorkingCandle(
                start=start, end=end, open=tick.ltp, high=tick.ltp, low=tick.ltp, close=tick.ltp
            )
            return None

        if start == w.start:
            if tick.ltp > w.high:
                w.high = tick.ltp
            if tick.ltp < w.low:
                w.low = tick.ltp
            w.close = tick.ltp
            return None

        # Candle rolled
        completed = Candle(
            start=w.start,
            end=w.end,
            open=w.open,
            high=w.high,
            low=w.low,
            close=w.close,
        )
        self._working = _WorkingCandle(
            start=start, end=end, open=tick.ltp, high=tick.ltp, low=tick.ltp, close=tick.ltp
        )
        return completed

