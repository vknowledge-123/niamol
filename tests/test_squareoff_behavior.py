from __future__ import annotations

import unittest
from datetime import datetime

from app.runtime.instruments import OptionContract
from app.runtime.settings import EngineConfig
from app.services.engine.controller import EngineController, _SimTrade
from app.services.engine.strategy import CloseLadder
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
    async def test_last_trade_stops_engine_after_exit(self) -> None:
        controller = EngineController(_DummyConfigStore(), _DummyInstruments(), kind="BUY")
        controller._running = True
        controller._run_mode = "SIM"

        contract = OptionContract(
            security_id="sec-1",
            trading_symbol="NIFTY-25300-CE",
            expiry=datetime(2026, 3, 6, 15, 30, 0),
            strike=25300,
            option_type="CE",
            lot_size=65,
        )
        tr = _SimTrade(
            id=1,
            side="CALL",
            strategy_side="CALL",
            trade_side="CALL",
            kind="BUY",
            contract=contract,
            fills=[],
        )
        controller._sim_trades = [tr]
        controller._sim_active = tr
        controller._active_contract = contract
        controller._option_ltps[contract.security_id] = 100.0

        cfg = EngineConfig(trading_enabled=True, last_trade=True)
        now = datetime(2026, 3, 6, 10, 0, 0)
        await controller._handle_actions(
            [CloseLadder(side="CALL", spot=25255.0, lots_open=1, reason="target", flip_to="PUT")],
            spot=25255.0,
            cfg=cfg,
            now=now,
        )

        self.assertFalse(controller._running)
        self.assertIsNone(controller._run_mode)
        self.assertIsNone(controller._active_contract)
        self.assertIsNone(controller._sim_active)
        self.assertIsNone(tr.flip_to)

    def test_strategy_sequence_add_and_tsl_bounce_buy(self) -> None:
        engine = StrategyEngine.for_engine_kind(kind="BUY")
        cfg = EngineConfig(
            trading_enabled=True,
            initial_tsl_points=5.0,
            sequence_tsl_diff_points=1.0,
            lots_per_add=1,
            max_adds=0,
            max_losses_per_day=5,
        )
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=95.0,
            high_premium=100.0,
            low_premium=100.0,
            adds_done=0,
            lots_open=1,
        )

        actions1 = engine.on_option_tick(premium_ltp=105.95, spot_ltp=25255.0, cfg=cfg)
        self.assertEqual(actions1, [])
        self.assertAlmostEqual(float(engine.stop_premium or 0.0), 100.95, places=6)

        actions2 = engine.on_option_tick(premium_ltp=106.0, spot_ltp=25260.0, cfg=cfg)
        self.assertEqual(len(actions2), 1)
        self.assertEqual(actions2[0].side, "CALL")
        self.assertEqual(actions2[0].levels, 1)
        self.assertEqual(engine.adds_done, 1)
        self.assertAlmostEqual(float(engine.stop_premium or 0.0), 100.0, places=6)
        self.assertAlmostEqual(float(engine.next_add_premium or 0.0), 113.0, places=6)

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
            adds_done=0,
            lots_open=1,
        )

        actions = engine.on_option_tick(premium_ltp=94.0, spot_ltp=25240.0, cfg=cfg)

        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0].side, "CALL")
        self.assertEqual(actions[0].flip_to, None)
        self.assertEqual(actions[1].side, "CALL")
        self.assertEqual(engine.active_side, "CALL")

    def test_strategy_stop_manual_waits_for_decision(self) -> None:
        engine = StrategyEngine.for_engine_kind(kind="BUY")
        cfg = EngineConfig(trading_enabled=True, trade_direction_continue=False, max_losses_per_day=5)
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=95.0,
            high_premium=105.0,
            low_premium=95.0,
            adds_done=0,
            lots_open=1,
        )

        actions = engine.on_option_tick(premium_ltp=94.0, spot_ltp=25240.0, cfg=cfg)

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].side, "CALL")
        self.assertEqual(actions[0].flip_to, None)
        self.assertEqual(engine.active_side, None)
        self.assertTrue(engine.has_pending_manual_decision())
        self.assertEqual(engine.mode, Mode.WAITING_MANUAL)

        # Manual continue should reopen CALL.
        cont = engine.manual_continue_same(spot=25240.0, cfg=cfg)
        self.assertEqual(len(cont), 1)
        self.assertEqual(cont[0].side, "CALL")
        self.assertEqual(engine.active_side, "CALL")

        # Force another manual stop and then manual flip should reopen PUT.
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=95.0,
            high_premium=105.0,
            low_premium=95.0,
            adds_done=0,
            lots_open=1,
        )
        engine._pending_manual_side = None
        engine.on_option_tick(premium_ltp=94.0, spot_ltp=25240.0, cfg=cfg)
        flip = engine.manual_flip_opposite(spot=25240.0, cfg=cfg)
        self.assertEqual(len(flip), 1)
        self.assertEqual(flip[0].side, "PUT")
        self.assertEqual(engine.active_side, "PUT")

    def test_strategy_stop_full_automation_flips(self) -> None:
        engine = StrategyEngine.for_engine_kind(kind="BUY")
        cfg = EngineConfig(trading_enabled=True, full_automation=True, trade_direction_continue=False, max_losses_per_day=5)
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=95.0,
            high_premium=105.0,
            low_premium=95.0,
            adds_done=0,
            lots_open=1,
        )

        actions = engine.on_option_tick(premium_ltp=94.0, spot_ltp=25240.0, cfg=cfg)

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].side, "CALL")
        self.assertEqual(actions[0].flip_to, "PUT")
        self.assertEqual(engine.active_side, "PUT")
        self.assertEqual(engine.mode, Mode.LADDER_PUT)

    def test_strategy_live_config_recomputes_stop(self) -> None:
        engine = StrategyEngine.for_engine_kind(kind="BUY")
        cfg = EngineConfig(trading_enabled=True, initial_tsl_points=10.0, sequence_tsl_diff_points=0.0)
        engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=0.0,
            high_premium=120.0,
            low_premium=100.0,
            adds_done=0,
            lots_open=1,
        )

        engine.apply_live_config(cfg)
        self.assertAlmostEqual(float(engine.stop_premium or 0.0), 110.0, places=6)

        cfg2 = cfg.model_copy(update={"initial_tsl_points": 15.0})
        engine.apply_live_config(cfg2)
        self.assertAlmostEqual(float(engine.stop_premium or 0.0), 105.0, places=6)

    async def test_strategy_manual_square_off_day_locks(self) -> None:
        controller = EngineController(_DummyConfigStore(), _DummyInstruments(), kind="BUY")
        controller._engine._ladder = LadderState(
            side="CALL",
            entry_spot=25255.0,
            entry_premium=100.0,
            stop_premium=90.0,
            high_premium=100.0,
            low_premium=100.0,
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

        async def _fake_enqueue(ops, *, cfg) -> None:
            seen.extend(ops)

        controller._enqueue_orders = _fake_enqueue  # type: ignore[method-assign]

        status = await controller.square_off_and_stop()

        self.assertTrue(status.running)
        self.assertTrue(status.day_locked)
        self.assertEqual(status.mode, Mode.DAY_LOCKED.value)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0][0], "SELL")
        self.assertEqual(seen[0][1], "sec-1")
        self.assertEqual(seen[0][2], 75)
