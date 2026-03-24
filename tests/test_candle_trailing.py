from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from app.runtime.settings import EngineConfig
from app.services.engine.strategy import AddLot, CloseLadder, LadderState, StrategyEngine
from app.services.market.models import Candle, SpotTick


IST = ZoneInfo("Asia/Kolkata")


class CandleTrailingTests(unittest.TestCase):
    def test_call_candle_add_requires_green_and_min_points(self) -> None:
        cfg = EngineConfig(trading_enabled=True, pcl_trailing=True, candle_add_min_points=5.0, lots_per_add=1, max_adds=10)
        engine = StrategyEngine.for_engine_kind(kind="BUY")
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25200.0,
            entry_premium=100.0,
            stop_premium=None,
            high_premium=100.0,
            low_premium=100.0,
            adds_done=0,
            lots_open=1,
        )

        c1 = Candle(
            start=datetime(2026, 3, 24, 9, 30, 0, tzinfo=IST),
            end=datetime(2026, 3, 24, 9, 31, 0, tzinfo=IST),
            open=25200.0,
            high=25230.0,
            low=25190.0,
            close=25120.0,  # red -> no add
        )
        self.assertEqual(engine.on_1m_candle(c1, cfg), [])

        c2 = Candle(
            start=datetime(2026, 3, 24, 9, 31, 0, tzinfo=IST),
            end=datetime(2026, 3, 24, 9, 32, 0, tzinfo=IST),
            open=25115.0,
            high=25130.0,
            low=25110.0,
            close=25120.0,  # green -> first green candle => add
        )
        out2 = engine.on_1m_candle(c2, cfg)
        self.assertEqual(len(out2), 1)
        self.assertIsInstance(out2[0], AddLot)

        c3 = Candle(
            start=datetime(2026, 3, 24, 9, 32, 0, tzinfo=IST),
            end=datetime(2026, 3, 24, 9, 33, 0, tzinfo=IST),
            open=25120.0,
            high=25130.0,
            low=25110.0,
            close=25124.0,  # green, +4 vs prev green close 25120 => no add
        )
        self.assertEqual(engine.on_1m_candle(c3, cfg), [])

        c4 = Candle(
            start=datetime(2026, 3, 24, 9, 33, 0, tzinfo=IST),
            end=datetime(2026, 3, 24, 9, 34, 0, tzinfo=IST),
            open=25124.0,
            high=25140.0,
            low=25120.0,
            close=25130.0,  # green, +6 vs prev green close 25124 => add
        )
        out4 = engine.on_1m_candle(c4, cfg)
        self.assertEqual(len(out4), 1)
        self.assertIsInstance(out4[0], AddLot)

    def test_call_candle_stop_trails_to_prev_candle_low_and_exits_on_break(self) -> None:
        cfg = EngineConfig(trading_enabled=True, pcl_trailing=True, candle_add_min_points=5.0, lots_per_add=1, full_automation=True)
        engine = StrategyEngine.for_engine_kind(kind="BUY")
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25200.0,
            entry_premium=100.0,
            stop_premium=None,
            high_premium=100.0,
            low_premium=100.0,
            adds_done=0,
            lots_open=1,
        )

        candle = Candle(
            start=datetime(2026, 3, 24, 9, 30, 0, tzinfo=IST),
            end=datetime(2026, 3, 24, 9, 31, 0, tzinfo=IST),
            open=25200.0,
            high=25230.0,
            low=25190.0,
            close=25210.0,
        )
        engine.on_1m_candle(candle, cfg)
        self.assertAlmostEqual(float(engine.stop_spot or 0.0), 25190.0 - 2.5, places=6)

        # Spot breaks below stop -> exit.
        out = engine.on_tick(SpotTick(ts=datetime(2026, 3, 24, 9, 31, 5, tzinfo=IST), ltp=25187.0), cfg)
        self.assertTrue(out)
        self.assertIsInstance(out[0], CloseLadder)
        self.assertTrue(str(out[0].reason).startswith("stop_"))
