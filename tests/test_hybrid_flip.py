from __future__ import annotations

import unittest
from datetime import datetime

from app.runtime.instruments import OptionContract
from app.runtime.settings import EngineConfig
from app.services.engine.hybrid_controller import HybridEngineController
from app.services.engine.strategy import CloseLadder, StrategyEngine
from app.services.market.models import SpotTick


class _DummyConfigStore:
    def __init__(self, cfg: EngineConfig) -> None:
        self._cfg = cfg

    def current(self) -> EngineConfig:
        return self._cfg

    def version(self) -> int:
        return 0

    async def get(self) -> EngineConfig:
        return self._cfg


class _DummyInstruments:
    pass


class HybridFlipTests(unittest.IsolatedAsyncioTestCase):
    async def test_hybrid_stop_flips_buy_to_sell_with_extra_lot(self) -> None:
        cfg = EngineConfig(
            trading_enabled=True,
            lots_per_add=2,  # base (should NOT be used for the extra-lot flip)
            hybrid={
                "execution_mode": "BUY",
                "call_buy": {"lots_per_add": 2},
                "call_sell": {"lots_per_add": 1},
                "put_buy": {"lots_per_add": 2},
                "put_sell": {"lots_per_add": 1},
            },
        )

        controller = HybridEngineController(_DummyConfigStore(cfg), _DummyInstruments())
        controller._running = True
        controller._run_mode = "LIVE"
        controller._kind = "BUY"
        controller._engine = StrategyEngine.for_engine_kind(kind="BUY")
        controller._engine.last_tick = SpotTick(ts=datetime(2026, 3, 23, 10, 0, 0), ltp=25255.0)
        controller._hybrid_last_base_cfg = cfg

        contract = OptionContract(
            security_id="sec-1",
            trading_symbol="NIFTY-25300-CE",
            expiry=datetime(2026, 3, 26, 15, 30, 0),
            strike=25300,
            option_type="CE",
            lot_size=75,
        )
        controller._active_contract = contract

        seen: list[tuple[str, str, int, str]] = []

        async def _fake_enqueue(ops, *, cfg) -> None:
            seen.extend(ops)

        controller._enqueue_orders = _fake_enqueue  # type: ignore[method-assign]

        await controller._handle_actions(
            [CloseLadder(side="CALL", spot=25240.0, lots_open=3, reason="stop_manual", flip_to=None)],
            spot=25240.0,
            cfg=cfg,
            now=datetime(2026, 3, 23, 10, 1, 0),
        )

        self.assertEqual(len(seen), 1)
        # 3 lots to close + 1 lot extra to start SELL ladder.
        self.assertEqual(seen[0][0], "SELL")
        self.assertEqual(seen[0][1], "sec-1")
        self.assertEqual(seen[0][2], 75 * (3 + 1))
        self.assertEqual(seen[0][3], "open_call_sell")

        self.assertEqual(controller._kind, "SELL")
        self.assertEqual(getattr(controller._engine, "_kind", None), "SELL")
        self.assertEqual(controller._engine.active_side, "CALL")
        self.assertEqual(controller._engine.lots_open, 1)

    async def test_hybrid_stop_flips_sell_to_buy_with_extra_lot(self) -> None:
        cfg = EngineConfig(
            trading_enabled=True,
            lots_per_add=2,
            hybrid={
                "execution_mode": "SELL",
                "call_buy": {"lots_per_add": 1},
                "call_sell": {"lots_per_add": 2},
                "put_buy": {"lots_per_add": 1},
                "put_sell": {"lots_per_add": 2},
            },
        )

        controller = HybridEngineController(_DummyConfigStore(cfg), _DummyInstruments())
        controller._running = True
        controller._run_mode = "LIVE"
        controller._kind = "SELL"
        controller._engine = StrategyEngine.for_engine_kind(kind="SELL")
        controller._engine.last_tick = SpotTick(ts=datetime(2026, 3, 23, 10, 0, 0), ltp=25255.0)
        controller._hybrid_last_base_cfg = cfg

        contract = OptionContract(
            security_id="sec-1",
            trading_symbol="NIFTY-25300-CE",
            expiry=datetime(2026, 3, 26, 15, 30, 0),
            strike=25300,
            option_type="CE",
            lot_size=75,
        )
        controller._active_contract = contract

        seen: list[tuple[str, str, int, str]] = []

        async def _fake_enqueue(ops, *, cfg) -> None:
            seen.extend(ops)

        controller._enqueue_orders = _fake_enqueue  # type: ignore[method-assign]

        await controller._handle_actions(
            [CloseLadder(side="CALL", spot=25240.0, lots_open=3, reason="stop_manual", flip_to=None)],
            spot=25240.0,
            cfg=cfg,
            now=datetime(2026, 3, 23, 10, 1, 0),
        )

        self.assertEqual(len(seen), 1)
        # 3 lots to close + 1 lot extra to start BUY ladder.
        self.assertEqual(seen[0][0], "BUY")
        self.assertEqual(seen[0][1], "sec-1")
        self.assertEqual(seen[0][2], 75 * (3 + 1))
        self.assertEqual(seen[0][3], "open_call_buy")

        self.assertEqual(controller._kind, "BUY")
        self.assertEqual(getattr(controller._engine, "_kind", None), "BUY")
        self.assertEqual(controller._engine.active_side, "CALL")
        self.assertEqual(controller._engine.lots_open, 1)

    async def test_hybrid_sim_pnl_and_position_use_trade_kind_not_current_kind(self) -> None:
        cfg = EngineConfig(
            trading_enabled=True,
            lots_per_add=1,
            hybrid={
                "execution_mode": "SELL",
                "call_buy": {"lots_per_add": 1},
                "call_sell": {"lots_per_add": 1},
                "put_buy": {"lots_per_add": 1},
                "put_sell": {"lots_per_add": 1},
            },
        )

        controller = HybridEngineController(_DummyConfigStore(cfg), _DummyInstruments())
        controller._running = True
        controller._run_mode = "SIM"
        controller._kind = "SELL"
        controller._engine = StrategyEngine.for_engine_kind(kind="SELL")
        controller._hybrid_last_base_cfg = cfg

        async def _noop_subscribe(*args, **kwargs) -> None:
            return None

        controller._ensure_option_subscription = _noop_subscribe  # type: ignore[method-assign]

        contract = OptionContract(
            security_id="sec-1",
            trading_symbol="NIFTY-22800-PE",
            expiry=datetime(2026, 3, 26, 15, 30, 0),
            strike=22800,
            option_type="PE",
            lot_size=65,
        )

        # Start with a PUT_SELL trade (SELL kind).
        controller._option_ltps[contract.security_id] = 112.53
        now0 = datetime(2026, 3, 23, 10, 0, 0)
        cfg_sell = controller._hybrid_cfg_for_leg(base_cfg=cfg, side="PUT", kind="SELL")
        await controller._sim_open_ladder_with_contract(side="PUT", spot=22778.35, contract=contract, cfg=cfg_sell, now=now0)

        # Stop hit -> flip into PUT_BUY (BUY kind) on the same contract.
        controller._option_ltps[contract.security_id] = 112.53
        await controller._handle_actions(
            [CloseLadder(side="PUT", spot=22778.35, lots_open=1, reason="stop_manual", flip_to=None)],
            spot=22778.35,
            cfg=cfg,
            now=datetime(2026, 3, 23, 10, 1, 0),
        )

        # Close the BUY leg at a lower premium -> should be a LOSS for BUY.
        controller._option_ltps[contract.security_id] = 91.05
        await controller._handle_actions(
            [CloseLadder(side="PUT", spot=22835.80, lots_open=1, reason="target", flip_to=None)],
            spot=22835.80,
            cfg=cfg,
            now=datetime(2026, 3, 23, 10, 2, 0),
        )

        # Reproduce the original bug surface: current controller kind differs from the historical trade kind.
        controller._kind = "SELL"

        trades = controller.sim_trades(limit=10)
        buy_trade = next(t for t in trades if t["side"] == "PUT_BUY")
        self.assertEqual(buy_trade["position"], "LONG")
        self.assertAlmostEqual(float(buy_trade["pnl"]), 65 * (91.05 - 112.53), places=6)
