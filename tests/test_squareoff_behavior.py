from __future__ import annotations

import unittest
from datetime import datetime

from app.runtime.instruments import OptionContract
from app.runtime.settings import EngineConfig
from app.services.engine.controller import EngineController
from app.services.engine.strategy import LadderState, Mode
from app.services.engine.strategy import StrategyEngine
from app.services.market.models import SpotTick


class _DummyConfigStore:
    def __init__(self) -> None:
        self._cfg = EngineConfig(trading_enabled=True)

    def current(self) -> EngineConfig:
        return self._cfg

    def version(self) -> int:
        return 0


class _DummyInstruments:
    pass


class _DummyRest:
    def __init__(self, net_qty: int = 75) -> None:
        self.net_qty = net_qty

    def get_net_position_qty(self, *, security_id: str) -> int:
        return self.net_qty


class SquareOffBehaviorTests(unittest.IsolatedAsyncioTestCase):
    def test_strategy_stop_continue_reopens_same_side(self) -> None:
        engine = StrategyEngine.for_engine_kind(kind="BUY")
        cfg = EngineConfig(trading_enabled=True, trade_direction_continue=True, max_losses_per_day=5)
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=95.0,
            high_premium=105.0,
            low_premium=95.0,
            next_add_level=1,
            adds_done=0,
            lots_open=1,
        )

        actions = engine.on_option_tick(premium_ltp=94.0, spot_ltp=25240.0, cfg=cfg)

        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0].side, "CALL")
        self.assertEqual(actions[0].flip_to, None)
        self.assertEqual(actions[1].side, "CALL")
        self.assertEqual(engine.active_side, "CALL")

    async def test_strategy_manual_square_off_day_locks(self) -> None:
        controller = EngineController(_DummyConfigStore(), _DummyInstruments(), kind="BUY")
        controller._engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=90.0,
            high_premium=100.0,
            low_premium=100.0,
            next_add_level=1,
            adds_done=0,
            lots_open=1,
        )

        actions = controller._engine.manual_square_off(spot=25255.0)

        self.assertEqual(len(actions), 1)
        self.assertTrue(controller._engine.day_locked)
        self.assertEqual(controller._engine.mode, Mode.DAY_LOCKED)

    async def test_square_off_stop_keeps_engine_running_and_day_locked(self) -> None:
        controller = EngineController(_DummyConfigStore(), _DummyInstruments(), kind="BUY")
        controller._running = True
        controller._run_mode = "LIVE"
        controller._rest = _DummyRest(net_qty=75)
        controller._engine.last_tick = SpotTick(ts=datetime(2026, 3, 6, 10, 0, 0), ltp=25255.0)
        controller._engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=90.0,
            high_premium=100.0,
            low_premium=100.0,
            next_add_level=1,
            adds_done=0,
            lots_open=1,
        )
        controller._active_contract = OptionContract(
            security_id="sec-1",
            trading_symbol="NIFTY-25300-CE",
            expiry=datetime(2026, 3, 12, 15, 30, 0),
            strike=25300,
            option_type="CE",
            lot_size=75,
        )

        seen: list[tuple[str, str, int, str]] = []

        async def _fake_wait(ops, *, cfg) -> None:
            seen.extend(ops)

        controller._enqueue_orders_and_wait = _fake_wait  # type: ignore[method-assign]

        status = await controller.square_off_and_stop()

        self.assertTrue(status.running)
        self.assertTrue(status.day_locked)
        self.assertEqual(status.mode, Mode.DAY_LOCKED.value)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0][0], "SELL")
        self.assertEqual(seen[0][1], "sec-1")
        self.assertEqual(seen[0][2], 75)
