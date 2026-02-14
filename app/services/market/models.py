from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SpotTick:
    ts: datetime  # timezone-aware (IST)
    ltp: float


@dataclass(frozen=True, slots=True)
class Candle:
    start: datetime  # inclusive, timezone-aware
    end: datetime  # exclusive, timezone-aware
    open: float
    high: float
    low: float
    close: float

    @property
    def green(self) -> bool:
        return self.close > self.open

    @property
    def red(self) -> bool:
        return self.close < self.open

