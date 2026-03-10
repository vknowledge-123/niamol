from __future__ import annotations

import unittest
from datetime import datetime

from app.runtime.instruments import OptionContract
from app.services.engine.controller import EngineController


class _DummyInstruments:
    async def get_weekly_option(self, *, now_ist: datetime, strike: int, option_type: str, expiry_offset: int) -> OptionContract:
        return OptionContract(
            security_id=f"{option_type}-{strike}",
            trading_symbol=f"NIFTY-{strike}-{option_type}",
            expiry=now_ist,
            strike=strike,
            option_type=option_type,  # type: ignore[arg-type]
            lot_size=75,
        )


class _Cfg:
    strike_step = 100
    weekly_expiry = "CURRENT"


class SellStrikeSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_sell_engine_prefers_otm_call_on_put_ladder(self) -> None:
        controller = EngineController(object(), _DummyInstruments(), kind="SELL")

        contract = await controller._select_option_contract(
            side="PUT",
            spot=25255,
            now=datetime(2026, 3, 6, 10, 0, 0),
            cfg=_Cfg(),
        )

        self.assertEqual(contract.option_type, "CE")
        self.assertEqual(contract.strike, 25300)

    async def test_sell_engine_prefers_otm_put_on_call_ladder(self) -> None:
        controller = EngineController(object(), _DummyInstruments(), kind="SELL")

        contract = await controller._select_option_contract(
            side="CALL",
            spot=25255,
            now=datetime(2026, 3, 6, 10, 0, 0),
            cfg=_Cfg(),
        )

        self.assertEqual(contract.option_type, "PE")
        self.assertEqual(contract.strike, 25200)

    async def test_sell_engine_uses_strict_otm_when_spot_is_exact_strike(self) -> None:
        controller = EngineController(object(), _DummyInstruments(), kind="SELL")

        call_contract = await controller._select_option_contract(
            side="PUT",
            spot=25200,
            now=datetime(2026, 3, 6, 10, 0, 0),
            cfg=_Cfg(),
        )
        put_contract = await controller._select_option_contract(
            side="CALL",
            spot=25200,
            now=datetime(2026, 3, 6, 10, 0, 0),
            cfg=_Cfg(),
        )

        self.assertEqual(call_contract.option_type, "CE")
        self.assertEqual(call_contract.strike, 25300)
        self.assertEqual(put_contract.option_type, "PE")
        self.assertEqual(put_contract.strike, 25100)
