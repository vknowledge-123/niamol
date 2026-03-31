from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Literal, Optional

from pydantic import AliasChoices
from pydantic import ConfigDict
from pydantic import BaseModel, Field

from app.runtime.paths import CONFIG_PATH
from app.runtime.persistence import read_json, write_json


class HybridLegConfig(BaseModel):
    """
    Hybrid engine overrides for a specific ladder leg (CALL/PUT x BUY/SELL).

    If a field is None, HybridEngine falls back to the base EngineConfig value.
    """

    model_config = ConfigDict(extra="ignore")

    lots_per_add: Optional[int] = None
    max_adds: Optional[int] = None
    target_points: Optional[float] = None
    initial_tsl_points: Optional[float] = None
    sequence_tsl_diff_points: Optional[float] = None


class HybridConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    # Which ladder type to start with when hybrid engine starts.
    # BUY = start with CALL_BUY/PUT_BUY; SELL = start with CALL_SELL/PUT_SELL.
    execution_mode: Literal["BUY", "SELL"] = "BUY"

    call_buy: HybridLegConfig = Field(default_factory=HybridLegConfig)
    call_sell: HybridLegConfig = Field(default_factory=HybridLegConfig)
    put_buy: HybridLegConfig = Field(default_factory=HybridLegConfig)
    put_sell: HybridLegConfig = Field(default_factory=HybridLegConfig)


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

    # Contract selection for option contracts.
    contract_kind: Literal["WEEKLY", "MONTHLY"] = "WEEKLY"

    # Weekly expiry selection for option contracts.
    weekly_expiry: Literal["CURRENT", "NEXT"] = "CURRENT"

    # Monthly expiry selection for option contracts.
    # 0 = current monthly, 1 = next monthly, ...
    monthly_expiry_offset: int = 0

    # Ladder parameters (in option premium points; allow decimals like 0.05)
    target_points: float = 50.0

    # Trailing SL / add sequencing (premium points)
    #
    # - initial_tsl_points: initial trailing distance at entry (BUY: entry - initial_tsl; SELL: entry + initial_tsl)
    # - sequence_tsl_diff_points: each time an ADD triggers, increase the trailing distance by this amount.
    #
    # Adds are also derived from this sequence (no separate add-step setting):
    # - BUY: next add triggers after cumulative favorable move of:
    #        sum_{k=1..n}(initial_tsl + k*diff)
    # - SELL: same, but downward (entry - cumulative)
    initial_tsl_points: float = 10.0
    sequence_tsl_diff_points: float = 1.0

    # Candle-based trailing stop (spot) + candle-only adds (1-minute IST candles).
    #
    # When enabled:
    # - CALL ladder stop trails to the previous 1m candle LOW (PCL).
    # - PUT ladder stop trails to the previous 1m candle HIGH (PDH).
    # - Lot additions happen only on 1m candle close (spot), with color/delta filters.
    pcl_trailing: bool = False
    pdh_trailing: bool = False
    candle_add_min_points: float = 5.0
    candle_stop_buffer_points: float = 2.5
    # Candle-add behavior:
    # - True  (default): allow a candle-close add even on the entry candle close.
    # - False: strictly require at least one full 1m candle after entry before the first add.
    candle_add_allow_same_entry_candle: bool = True

    max_losses_per_day: int = 5

    # If enabled, always auto-flip ladder direction on TSL/SL hit (CALL <-> PUT).
    # If disabled, behavior depends on `trade_direction_continue`:
    # - If `trade_direction_continue` is ON: auto re-enter the same side.
    # - Else: pause in `waiting_manual` and wait for user action (Flip opposite / Continue same).
    full_automation: bool = False
    # If enabled, on TSL/SL hit automatically re-enter the same ladder side (no flip).
    # If disabled, on TSL/SL hit the engine pauses and waits for a manual decision
    # (Flip opposite / Continue same from the UI).
    trade_direction_continue: bool = False

    # BUY engine only (LIVE): when enabled, after the first real ladder starts,
    # the opposite ladder side is monitored "ghost" (no broker orders), while the
    # starting side continues to place real orders on flips back.
    ghost_monitoring: bool = False

    # If enabled, after the current ladder exits on target or TSL/stop,
    # the controller stops the engine (no further trades until manual start).
    last_trade: bool = False

    # Order params
    order_type: Literal["MARKET", "LIMIT"] = "MARKET"
    limit_price_offset: float = 0.0
    lots_per_add: int = 1
    # Semantics: max_adds <= 0 disables adds (single entry only).
    max_adds: int = 0

    # If enabled, confirm close quantity using broker position lookup (slower, but can be more accurate).
    # If disabled, square-off uses local qty derived from ladder state (ultra-low latency).
    broker_qty_lookup: bool = False

    # Dhan instrument ids (optional overrides)
    spot_security_id: str = Field(
        default="13",
        validation_alias=AliasChoices("spot_security_id", "nifty_spot_security_id"),
    )

    # Hybrid engine settings (kept separate so existing BUY/SELL engines remain unchanged).
    hybrid: HybridConfig = Field(default_factory=HybridConfig)


class EngineStatus(BaseModel):
    running: bool
    engine_kind: Optional[str] = None  # BUY / SELL
    underlying: Optional[str] = None  # NIFTY / BANKNIFTY
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
    contract_kind: Optional[str] = None
    weekly_expiry: Optional[str] = None
    monthly_expiry_offset: Optional[int] = None

    # Premium-driven ladder tracking (best-effort; based on option LTP ticks).
    entry_premium: Optional[float] = None
    stop_premium: Optional[float] = None
    next_add_premium: Optional[float] = None

    # Ghost monitoring (BUY engine only; LIVE mode).
    ghost_monitoring: bool = False
    ghost_active: bool = False
    ghost_side: Optional[str] = None
    last_error: Optional[str] = None


class EngineConfigStore:
    def __init__(self, path: Path | None = None, *, default_cfg: EngineConfig | None = None) -> None:
        self._lock = asyncio.Lock()
        self._path = path or CONFIG_PATH
        self._default_cfg = default_cfg or EngineConfig()
        loaded = read_json(self._path)
        if loaded is not None:
            try:
                self._cfg = EngineConfig.model_validate(loaded)
            except Exception:
                self._cfg = self._default_cfg
        else:
            self._cfg = self._default_cfg
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
