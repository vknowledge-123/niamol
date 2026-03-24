from __future__ import annotations

import unittest
from datetime import datetime

from app.runtime.instruments import OptionContract
from app.services.engine.controller import EngineController


class _DummyInstruments:
    async def get_monthly_option(
        self, *, symbol: str, now_ist: datetime, strike: int, option_type: str, expiry_offset: int
    ) -> OptionContract:
        return OptionContract(
            security_id=f"{symbol}-{option_type}-{strike}",
            trading_symbol=f"{symbol}-{strike}-{option_type}",
            expiry=now_ist,
            strike=strike,
            option_type=option_type,  # type: ignore[arg-type]
            lot_size=15,
        )


class _Cfg:
    strike_step = 50
    weekly_expiry = "CURRENT"
    contract_kind = "WEEKLY"
    monthly_expiry_offset = 0


class BankNiftyStrikeSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_banknifty_buy_selects_two_strikes_otm(self) -> None:
        controller = EngineController(object(), _DummyInstruments(), kind="BUY", underlying="BANKNIFTY")

        now = datetime(2026, 3, 17, 10, 0, 0)
        call_contract = await controller._select_option_contract(side="CALL", spot=65124, now=now, cfg=_Cfg())
        put_contract = await controller._select_option_contract(side="PUT", spot=65124, now=now, cfg=_Cfg())

        self.assertEqual(call_contract.option_type, "CE")
        self.assertEqual(call_contract.strike, 65300)
        self.assertEqual(put_contract.option_type, "PE")
        self.assertEqual(put_contract.strike, 65000)

    async def test_banknifty_buy_strict_otm_on_exact_strike(self) -> None:
        controller = EngineController(object(), _DummyInstruments(), kind="BUY", underlying="BANKNIFTY")

        now = datetime(2026, 3, 17, 10, 0, 0)
        call_contract = await controller._select_option_contract(side="CALL", spot=63200, now=now, cfg=_Cfg())
        put_contract = await controller._select_option_contract(side="PUT", spot=63200, now=now, cfg=_Cfg())

        self.assertEqual(call_contract.option_type, "CE")
        self.assertEqual(call_contract.strike, 63400)
        self.assertEqual(put_contract.option_type, "PE")
        self.assertEqual(put_contract.strike, 63000)

    async def test_banknifty_sell_selects_two_strikes_otm_for_sold_option(self) -> None:
        controller = EngineController(object(), _DummyInstruments(), kind="SELL", underlying="BANKNIFTY")

        now = datetime(2026, 3, 17, 10, 0, 0)
        # SELL engine swaps CALL/PUT option type per ladder side.
        call_ladder_contract = await controller._select_option_contract(side="CALL", spot=65124, now=now, cfg=_Cfg())
        put_ladder_contract = await controller._select_option_contract(side="PUT", spot=65124, now=now, cfg=_Cfg())

        self.assertEqual(call_ladder_contract.option_type, "PE")
        self.assertEqual(call_ladder_contract.strike, 65000)
        self.assertEqual(put_ladder_contract.option_type, "CE")
        self.assertEqual(put_ladder_contract.strike, 65300)

