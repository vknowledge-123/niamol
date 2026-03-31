from __future__ import annotations

import unittest
from datetime import datetime

from app.runtime.instruments import OptionContract
from app.runtime.settings import EngineConfig
from app.services.engine.controller import EngineController


class _DummyConfigStore:
    def __init__(self) -> None:
        self._cfg = EngineConfig(trading_enabled=True)

    def current(self) -> EngineConfig:
        return self._cfg

    def version(self) -> int:
        return 0


class _DummyInstruments:
    pass


class _DummyFeed:
    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []

    async def subscribe_option(self, security_id: str) -> None:
        self.subscribed.append(str(security_id))

    async def unsubscribe_option(self, security_id: str) -> None:
        self.unsubscribed.append(str(security_id))


class OptionLtpCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_unsubscribe_clears_cached_ltp_for_old_contract(self) -> None:
        controller = EngineController(_DummyConfigStore(), _DummyInstruments(), kind="BUY")
        controller._feed = _DummyFeed()

        old = OptionContract(
            security_id="000123",
            trading_symbol="BANKNIFTY-52600-CE",
            expiry=datetime(2026, 3, 27, 15, 30, 0),
            strike=52600,
            option_type="CE",
            lot_size=30,
        )
        new = OptionContract(
            security_id="456",
            trading_symbol="BANKNIFTY-52300-PE",
            expiry=datetime(2026, 3, 27, 15, 30, 0),
            strike=52300,
            option_type="PE",
            lot_size=30,
        )

        controller._option_ltps["000123"] = 101.0
        controller._option_ltps["123"] = 100.0
        controller._pending_exec_entry_premiums["000123"] = 101.0
        controller._pending_exec_entry_premiums["123"] = 100.0

        await controller._ensure_option_subscription(new, unsubscribe_old=old)

        self.assertNotIn("000123", controller._option_ltps)
        self.assertNotIn("123", controller._option_ltps)
        self.assertNotIn("000123", controller._pending_exec_entry_premiums)
        self.assertNotIn("123", controller._pending_exec_entry_premiums)
