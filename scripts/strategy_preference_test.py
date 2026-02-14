from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.runtime.settings import EngineConfig
from app.services.engine.strategy import OpenLadder, StrategyEngine
from app.services.market.models import Candle, SpotTick


IST = ZoneInfo("Asia/Kolkata")


def make_candle(start: datetime, o: float, h: float, l: float, c: float) -> Candle:
    return Candle(start=start, end=start + timedelta(minutes=1), open=o, high=h, low=l, close=c)


def main() -> None:
    # Preference CALL: red setup must NOT arm; green must arm.
    eng = StrategyEngine()
    cfg = EngineConfig(trading_enabled=True, start_preference="CALL")

    t0 = datetime(2026, 2, 16, 9, 30, tzinfo=IST)
    eng.on_candle(make_candle(t0, 100, 101, 98, 99), cfg)  # red
    eng.on_candle(make_candle(t0 + timedelta(minutes=1), 99, 100, 95, 96), cfg)  # red
    actions = eng.on_tick(SpotTick(ts=t0 + timedelta(minutes=2), ltp=94), cfg)
    assert not any(isinstance(a, OpenLadder) for a in actions), actions

    eng.on_candle(make_candle(t0 + timedelta(minutes=2), 100, 105, 99, 104), cfg)  # green
    eng.on_candle(make_candle(t0 + timedelta(minutes=3), 104, 108, 103, 107), cfg)  # green (trigger=108)
    actions = eng.on_tick(SpotTick(ts=t0 + timedelta(minutes=4), ltp=109), cfg)
    assert any(isinstance(a, OpenLadder) and a.side == "CALL" for a in actions), actions

    print("PREFERENCE OK")


if __name__ == "__main__":
    main()
