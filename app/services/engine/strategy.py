from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Deque, Literal, Optional, TypeAlias

from app.runtime.settings import EngineConfig
from app.services.market.models import Candle, SpotTick


LadderSide = Literal["CALL", "PUT"]
DayLockReason = Literal["target", "max_losses"]


class Mode(str, Enum):
    WAITING_BREAKOUT = "waiting_breakout"
    LADDER_CALL = "ladder_call"
    LADDER_PUT = "ladder_put"
    DAY_LOCKED = "day_locked"


@dataclass(frozen=True, slots=True)
class BreakoutSetup:
    side: LadderSide
    trigger: float  # spot level
    formed_at: datetime  # candle2 end time


@dataclass(frozen=True, slots=True)
class OpenLadder:
    side: LadderSide
    spot: float


@dataclass(frozen=True, slots=True)
class AddLot:
    side: LadderSide
    spot: float
    levels: int


@dataclass(frozen=True, slots=True)
class CloseLadder:
    side: LadderSide
    spot: float
    lots_open: int
    reason: str
    flip_to: Optional[LadderSide]


Action: TypeAlias = OpenLadder | AddLot | CloseLadder


@dataclass(slots=True)
class LadderState:
    side: LadderSide
    entry_spot: float
    stop_spot: float
    high_watermark: float
    low_watermark: float
    next_add_level: int
    lots_open: int


class StrategyEngine:
    def __init__(self) -> None:
        self.mode: Mode = Mode.WAITING_BREAKOUT
        # Store a small rolling window; actual consecutive requirement is taken from config.
        self._candles: Deque[Candle] = deque(maxlen=20)
        self._setup: Optional[BreakoutSetup] = None
        self._ladder: Optional[LadderState] = None

        self.loss_count: int = 0
        self.day_locked: bool = False
        self.day_lock_reason: Optional[DayLockReason] = None
        self._started_once: bool = False
        self.last_tick: Optional[SpotTick] = None

    @property
    def active_side(self) -> Optional[LadderSide]:
        if self._ladder is None:
            return None
        return self._ladder.side

    @property
    def entry_spot(self) -> Optional[float]:
        return None if self._ladder is None else self._ladder.entry_spot

    @property
    def stop_spot(self) -> Optional[float]:
        return None if self._ladder is None else self._ladder.stop_spot

    @property
    def lots_open(self) -> int:
        return 0 if self._ladder is None else int(self._ladder.lots_open)

    @property
    def next_add_spot(self) -> Optional[float]:
        if self._ladder is None:
            return None
        cfg_add = self._last_cfg_add_step
        if cfg_add is None:
            return None
        if self._ladder.side == "CALL":
            return self._ladder.entry_spot + (self._ladder.next_add_level * cfg_add)
        return self._ladder.entry_spot - (self._ladder.next_add_level * cfg_add)

    _last_cfg_add_step: Optional[int] = None

    def reset_day(self) -> None:
        self.mode = Mode.WAITING_BREAKOUT
        self._candles.clear()
        self._setup = None
        self._ladder = None
        self.loss_count = 0
        self.day_locked = False
        self.day_lock_reason = None
        self._started_once = False

    def maybe_unlock_day(self, cfg: EngineConfig) -> bool:
        """
        If the engine was day-locked due to max-losses and the user increases
        `max_losses_per_day`, unlock immediately to allow trading again.
        """
        if not self.day_locked:
            return False
        if self.day_lock_reason != "max_losses":
            return False
        if self.loss_count >= int(cfg.max_losses_per_day):
            return False

        self.day_locked = False
        self.day_lock_reason = None
        self.mode = Mode.WAITING_BREAKOUT
        self._setup = None
        self._candles.clear()
        self._started_once = False
        return True

    def on_candle(self, candle: Candle, cfg: EngineConfig) -> None:
        if self.day_locked or self._started_once:
            return

        self._candles.append(candle)
        n = int(getattr(cfg, "require_two_consecutive", 2) or 2)
        if n < 2:
            n = 2
        while len(self._candles) > n:
            self._candles.popleft()
        if len(self._candles) < n:
            return

        seq = list(self._candles)
        last = seq[-1]
        pref = getattr(cfg, "start_preference", "AUTO")
        if pref not in ("AUTO", "CALL", "PUT"):
            pref = "AUTO"

        if (pref in ("AUTO", "CALL")) and all(c.green for c in seq):
            self._setup = BreakoutSetup(side="CALL", trigger=last.high, formed_at=last.end)
            return

        if (pref in ("AUTO", "PUT")) and all(c.red for c in seq):
            self._setup = BreakoutSetup(side="PUT", trigger=last.low, formed_at=last.end)
            return

        self._setup = None

    def on_tick(self, tick: SpotTick, cfg: EngineConfig) -> list[Action]:
        self.last_tick = tick
        self._last_cfg_add_step = cfg.add_step_points

        if self.day_locked:
            self.mode = Mode.DAY_LOCKED
            return []

        actions: list[Action] = []

        if self._ladder is None:
            self.mode = Mode.WAITING_BREAKOUT
            setup = self._setup
            if setup is None or not cfg.trading_enabled:
                return []

            if setup.side == "CALL" and tick.ltp > setup.trigger:
                actions.append(OpenLadder(side="CALL", spot=tick.ltp))
                self._open_ladder(side="CALL", spot=tick.ltp, cfg=cfg)
            elif setup.side == "PUT" and tick.ltp < setup.trigger:
                actions.append(OpenLadder(side="PUT", spot=tick.ltp))
                self._open_ladder(side="PUT", spot=tick.ltp, cfg=cfg)
            return actions

        # Ladder running
        ladder = self._ladder
        self.mode = Mode.LADDER_CALL if ladder.side == "CALL" else Mode.LADDER_PUT

        favorable = (tick.ltp - ladder.entry_spot) if ladder.side == "CALL" else (ladder.entry_spot - tick.ltp)

        if favorable >= cfg.target_points:
            actions.append(
                CloseLadder(side=ladder.side, spot=tick.ltp, lots_open=ladder.lots_open, reason="target", flip_to=None)
            )
            self._close_and_lock_day(reason="target")
            return actions

        # Continuous trailing stop (watermark-based), tick-by-tick.
        # CALL: track highest spot since entry -> stop = high - trail
        # PUT: track lowest spot since entry -> stop = low + trail
        trail = int(cfg.trail_step_points)
        if trail > 0:
            if ladder.side == "CALL":
                if tick.ltp > ladder.high_watermark:
                    ladder.high_watermark = tick.ltp
                if (ladder.high_watermark - ladder.entry_spot) >= trail:
                    new_stop = float(ladder.high_watermark - trail)
                    if new_stop > ladder.stop_spot:
                        ladder.stop_spot = new_stop
            else:
                if tick.ltp < ladder.low_watermark:
                    ladder.low_watermark = tick.ltp
                if (ladder.entry_spot - ladder.low_watermark) >= trail:
                    new_stop = float(ladder.low_watermark + trail)
                    if new_stop < ladder.stop_spot:
                        ladder.stop_spot = new_stop

        # Pyramiding at each add_step_points in favorable direction
        if cfg.add_step_points > 0:
            reached_level = int(favorable // cfg.add_step_points)
            if reached_level >= ladder.next_add_level:
                levels_to_add = reached_level - ladder.next_add_level + 1
                ladder.next_add_level = reached_level + 1
                ladder.lots_open += levels_to_add * cfg.lots_per_add
                actions.append(AddLot(side=ladder.side, spot=tick.ltp, levels=levels_to_add))

        # Stop check
        if ladder.side == "CALL" and tick.ltp <= ladder.stop_spot:
            actions.extend(self._handle_stop_hit(exit_spot=tick.ltp, cfg=cfg))
        elif ladder.side == "PUT" and tick.ltp >= ladder.stop_spot:
            actions.extend(self._handle_stop_hit(exit_spot=tick.ltp, cfg=cfg))

        return actions

    def _open_ladder(self, *, side: LadderSide, spot: float, cfg: EngineConfig) -> None:
        stop = (spot - cfg.initial_sl_points) if side == "CALL" else (spot + cfg.initial_sl_points)
        self._ladder = LadderState(
            side=side,
            entry_spot=spot,
            stop_spot=stop,
            high_watermark=spot,
            low_watermark=spot,
            next_add_level=1,
            lots_open=cfg.lots_per_add,
        )
        self._setup = None
        self._started_once = True

    def _handle_stop_hit(self, *, exit_spot: float, cfg: EngineConfig) -> list[Action]:
        ladder = self._ladder
        if ladder is None:
            return []

        pnl_points = (exit_spot - ladder.entry_spot) if ladder.side == "CALL" else (ladder.entry_spot - exit_spot)
        is_loss = pnl_points <= 0

        new_loss_count = self.loss_count + (1 if is_loss else 0)
        if new_loss_count >= cfg.max_losses_per_day:
            self.loss_count = new_loss_count
            out = [
                CloseLadder(
                    side=ladder.side, spot=exit_spot, lots_open=ladder.lots_open, reason="stop_max_losses", flip_to=None
                )
            ]
            self._close_and_lock_day(reason="max_losses")
            return out

        self.loss_count = new_loss_count
        flip_to: LadderSide = "PUT" if ladder.side == "CALL" else "CALL"
        out = [
            CloseLadder(side=ladder.side, spot=exit_spot, lots_open=ladder.lots_open, reason="stop_flip", flip_to=flip_to)
        ]
        self._open_ladder(side=flip_to, spot=exit_spot, cfg=cfg)
        return out

    def _close_and_lock_day(self, *, reason: DayLockReason) -> None:
        self._ladder = None
        self.day_locked = True
        self.day_lock_reason = reason
        self.mode = Mode.DAY_LOCKED
