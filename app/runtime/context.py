from __future__ import annotations

import asyncio

from app.runtime.instruments import InstrumentStore
from app.runtime.paths import BANK_CONFIG_PATH
from app.runtime.settings import EngineConfig, EngineConfigStore
from app.services.engine.controller import EngineController
from app.services.engine.hybrid_controller import HybridEngineController
from app.services.market.spot_candle_service import SpotCandleService


class AppContext:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.config_store = EngineConfigStore()
        self.bank_config_store = EngineConfigStore(
            path=BANK_CONFIG_PATH,
            default_cfg=EngineConfig(
                strike_step=100,
                contract_kind="MONTHLY",
                monthly_expiry_offset=0,
                spot_security_id="25",
            ),
        )
        self.instruments = InstrumentStore()
        self.spot_candles = SpotCandleService()
        self.engine = EngineController(self.config_store, self.instruments, underlying="NIFTY", spot_candles=self.spot_candles)
        self.sell_engine = EngineController(
            self.config_store, self.instruments, kind="SELL", underlying="NIFTY", spot_candles=self.spot_candles
        )
        self.hybrid_engine = HybridEngineController(
            self.config_store, self.instruments, underlying="NIFTY", spot_candles=self.spot_candles
        )
        self.bank_engine = EngineController(
            self.bank_config_store, self.instruments, underlying="BANKNIFTY", spot_candles=self.spot_candles
        )
        self.bank_sell_engine = EngineController(
            self.bank_config_store, self.instruments, kind="SELL", underlying="BANKNIFTY", spot_candles=self.spot_candles
        )
        self.bank_hybrid_engine = HybridEngineController(
            self.bank_config_store, self.instruments, underlying="BANKNIFTY", spot_candles=self.spot_candles
        )

    async def startup(self) -> None:
        await self.instruments.load_from_disk_if_present()
        await self.refresh_spot_candles()

    async def shutdown(self) -> None:
        await self.engine.stop()
        await self.sell_engine.stop()
        await self.hybrid_engine.stop()
        await self.bank_engine.stop()
        await self.bank_sell_engine.stop()
        await self.bank_hybrid_engine.stop()
        await self.spot_candles.stop()

    async def refresh_spot_candles(self) -> None:
        """
        Ensure the background spot candle service is running with the latest
        credentials and spot security ids.
        """
        cfg = self.config_store.current()
        bcfg = self.bank_config_store.current()

        client_id = cfg.client_id or bcfg.client_id
        access_token = cfg.access_token or bcfg.access_token
        if not client_id or not access_token:
            return

        # Prefer spot security ids from the scrip master (when loaded) so the
        # candle service and engines always agree on the spot_sid values.
        nifty_sid = str(cfg.spot_security_id)
        bank_sid = str(bcfg.spot_security_id)
        try:
            if self.instruments.loaded:
                nifty_sid = str(await self.instruments.spot_security_id(symbol="NIFTY", default=nifty_sid))
                bank_sid = str(await self.instruments.spot_security_id(symbol="BANKNIFTY", default=bank_sid))
        except Exception:
            pass
        await self.spot_candles.start(
            client_id=str(client_id),
            access_token=str(access_token),
            nifty_spot_security_id=nifty_sid,
            bank_spot_security_id=bank_sid,
        )
