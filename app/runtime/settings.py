from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Literal, Optional

from pydantic import ConfigDict
from pydantic import BaseModel, Field

from app.runtime.paths import CONFIG_PATH
from app.runtime.persistence import read_json, write_json


class EngineConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    client_id: Optional[str] = None
    access_token: Optional[str] = None

    trading_enabled: bool = False

    # Startup preference: when waiting for first breakout after engine start,
    # restrict to a specific ladder side or allow both.
    start_preference: Literal["AUTO", "CALL", "PUT"] = "AUTO"

    # If enabled, bypass candle-breakout entry and start the first ladder immediately
    # (uses `start_preference` to pick CALL/PUT; AUTO keeps normal breakout behavior).
    instant_start: bool = False

    # Candle / entry
    timeframe_seconds: int = 60
    require_two_consecutive: int = 2

    # Strike selection
    strike_step: int = 100

    # Weekly expiry selection for option contracts.
    weekly_expiry: Literal["CURRENT", "NEXT"] = "CURRENT"

    # Ladder parameters (in option premium points; allow decimals like 0.05)
    add_step_points: float = 10.0
    target_points: float = 50.0
    trail_step_points: float = 10.0
    initial_sl_points: float = 10.0

    max_losses_per_day: int = 5

    # Order params
    order_type: Literal["MARKET", "LIMIT"] = "MARKET"
    limit_price_offset: float = 0.0
    lots_per_add: int = 1
    max_adds: int = 0  # 0 = unlimited

    # Dhan instrument ids (optional overrides)
    nifty_spot_security_id: str = "13"


class EngineStatus(BaseModel):
    running: bool
    engine_kind: Optional[str] = None  # BUY / SELL
    position: Optional[str] = None  # LONG / SHORT
    trading_enabled: bool
    mode: str
    active_ladder: Optional[str]
    spot_ltp: Optional[float]
    entry_spot: Optional[float]
    stop_spot: Optional[float]
    next_add_spot: Optional[float]
    lots_open: int
    adds_done: int = 0
    max_adds: int = 0
    loss_count: int
    day_locked: bool
    active_contract_symbol: Optional[str] = None
    active_contract_security_id: Optional[str] = None
    active_option_ltp: Optional[float] = None
    active_contract_expiry: Optional[str] = None
    active_contract_strike: Optional[int] = None
    active_contract_option_type: Optional[str] = None
    active_contract_lot_size: Optional[int] = None
    active_qty: Optional[int] = None
    weekly_expiry: Optional[str] = None

    # Premium-driven ladder tracking (best-effort; based on option LTP ticks).
    entry_premium: Optional[float] = None
    stop_premium: Optional[float] = None
    next_add_premium: Optional[float] = None
    last_error: Optional[str] = None


class EngineConfigStore:
    def __init__(self, path: Path | None = None) -> None:
        self._lock = asyncio.Lock()
        self._path = path or CONFIG_PATH
        loaded = read_json(self._path)
        if loaded is not None:
            try:
                self._cfg = EngineConfig.model_validate(loaded)
            except Exception:
                self._cfg = EngineConfig()
        else:
            self._cfg = EngineConfig()
        self._version = 0

    def current(self) -> EngineConfig:
        # Read-only snapshot for hot-path usage (do not mutate).
        return self._cfg

    def version(self) -> int:
        return self._version

    async def get(self) -> EngineConfig:
        async with self._lock:
            return self._cfg.model_copy(deep=True)

    async def set(self, new_cfg: EngineConfig) -> EngineConfig:
        async with self._lock:
            self._cfg = new_cfg
            self._version += 1
            write_json(self._path, self._cfg.model_dump())
            return self._cfg.model_copy(deep=True)

    async def patch(self, **kwargs) -> EngineConfig:
        async with self._lock:
            updated = self._cfg.model_copy(update=kwargs)
            self._cfg = updated
            self._version += 1
            write_json(self._path, self._cfg.model_dump())
            return self._cfg.model_copy(deep=True)
