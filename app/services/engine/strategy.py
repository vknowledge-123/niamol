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
    entry_premium: Optional[float]
    stop_premium: Optional[float]
    high_premium: Optional[float]
    low_premium: Optional[float]
    next_add_level: int
    adds_done: int
    lots_open: int


class StrategyEngine:
    def __init__(self, *, kind: str = "BUY") -> None:
        self._kind = "SELL" if str(kind).upper() == "SELL" else "BUY"
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

    @classmethod
    def for_engine_kind(cls, *, kind: str) -> "StrategyEngine":
        """
        Factory hook for engine-kind specific behavior.

        NOTE: Candle criteria are enforced to *start* a ladder (via on_candle/setup).
        After a ladder is running, a TSL/SL hit flips immediately without re-checking
        candle criteria (for both BUY and SELL engines).
        """
        return cls(kind=kind)

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
        # Spot-based stop levels are no longer used; trailing/target/add are premium-driven.
        return None

    @property
    def lots_open(self) -> int:
        return 0 if self._ladder is None else int(self._ladder.lots_open)

    @property
    def adds_done(self) -> int:
        return 0 if self._ladder is None else int(self._ladder.adds_done)

    @property
    def next_add_spot(self) -> Optional[float]:
        # Spot-based add levels are no longer used; trailing/target/add are premium-driven.
        return None

    @property
    def entry_premium(self) -> Optional[float]:
        return None if self._ladder is None else self._ladder.entry_premium

    @property
    def stop_premium(self) -> Optional[float]:
        return None if self._ladder is None else self._ladder.stop_premium

    @property
    def next_add_premium(self) -> Optional[float]:
        ladder = self._ladder
        if ladder is None or ladder.entry_premium is None:
            return None
        cfg_add = self._last_cfg_add_step
        if cfg_add is None:
            return None
        entry = float(ladder.entry_premium)
        if self._kind == "BUY":
            return entry + (ladder.next_add_level * float(cfg_add))
        return entry - (ladder.next_add_level * float(cfg_add))

    _last_cfg_add_step: Optional[float] = None

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

    def force_unlock_day(self) -> bool:
        """
        Manually unlock the day after a day-lock (e.g. after target or max-losses),
        returning the engine to breakout monitoring.

        Note: this does not change `loss_count`.
        """
        if not self.day_locked:
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

        if bool(getattr(cfg, "instant_start", False)) and getattr(cfg, "start_preference", "AUTO") in ("CALL", "PUT"):
            # Instant-start bypasses candle breakout setup.
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
            if cfg.trading_enabled and bool(getattr(cfg, "instant_start", False)):
                pref = getattr(cfg, "start_preference", "AUTO")
                if pref in ("CALL", "PUT"):
                    actions.append(OpenLadder(side=pref, spot=tick.ltp))
                    self._open_ladder(side=pref, spot=tick.ltp, cfg=cfg)
                    return actions

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

        # Ladder running: manage adds/stop/target on option premium ticks, not spot ticks.
        ladder = self._ladder
        self.mode = Mode.LADDER_CALL if ladder.side == "CALL" else Mode.LADDER_PUT
        return []

    def on_option_tick(self, *, premium_ltp: float, spot_ltp: float, cfg: EngineConfig) -> list[Action]:
        """
        Manage ladder lifecycle using option premium movement (adds, trailing stop, target).
        """
        if self.day_locked:
            self.mode = Mode.DAY_LOCKED
            return []

        ladder = self._ladder
        if ladder is None:
            self.mode = Mode.WAITING_BREAKOUT
            return []

        self.mode = Mode.LADDER_CALL if ladder.side == "CALL" else Mode.LADDER_PUT

        prem = float(premium_ltp)
        actions: list[Action] = []

        if ladder.entry_premium is None:
            ladder.entry_premium = prem
            ladder.high_premium = prem
            ladder.low_premium = prem
            if self._kind == "BUY":
                ladder.stop_premium = prem - float(cfg.initial_sl_points)
            else:
                ladder.stop_premium = prem + float(cfg.initial_sl_points)
            return []

        entry = float(ladder.entry_premium)
        favorable = (prem - entry) if self._kind == "BUY" else (entry - prem)

        if favorable >= float(cfg.target_points):
            actions.append(CloseLadder(side=ladder.side, spot=float(spot_ltp), lots_open=ladder.lots_open, reason="target", flip_to=None))
            self._close_and_lock_day(reason="target")
            return actions

        trail = float(cfg.trail_step_points)
        if trail > 0:
            if self._kind == "BUY":
                if ladder.high_premium is None or prem > float(ladder.high_premium):
                    ladder.high_premium = prem
                new_stop = float(ladder.high_premium) - trail
                if ladder.stop_premium is None or new_stop > float(ladder.stop_premium):
                    ladder.stop_premium = new_stop
            else:
                if ladder.low_premium is None or prem < float(ladder.low_premium):
                    ladder.low_premium = prem
                new_stop = float(ladder.low_premium) + trail
                if ladder.stop_premium is None or new_stop < float(ladder.stop_premium):
                    ladder.stop_premium = new_stop

        if cfg.add_step_points > 0:
            reached_level = int(favorable // float(cfg.add_step_points))
            if reached_level >= ladder.next_add_level:
                planned_levels = reached_level - ladder.next_add_level + 1
                max_adds = int(getattr(cfg, "max_adds", 0) or 0)
                remaining = None if max_adds <= 0 else max(0, max_adds - int(ladder.adds_done))
                levels_to_add = planned_levels if remaining is None else min(planned_levels, remaining)

                ladder.next_add_level = reached_level + 1
                if levels_to_add > 0:
                    ladder.adds_done += int(levels_to_add)
                    ladder.lots_open += levels_to_add * cfg.lots_per_add
                    actions.append(AddLot(side=ladder.side, spot=float(spot_ltp), levels=levels_to_add))

        stop = ladder.stop_premium
        if stop is not None:
            if self._kind == "BUY" and prem <= float(stop):
                actions.extend(self._handle_stop_hit(exit_premium=prem, spot_ltp=float(spot_ltp), cfg=cfg))
            elif self._kind == "SELL" and prem >= float(stop):
                actions.extend(self._handle_stop_hit(exit_premium=prem, spot_ltp=float(spot_ltp), cfg=cfg))

        return actions

    def _open_ladder(self, *, side: LadderSide, spot: float, cfg: EngineConfig) -> None:
        self._ladder = LadderState(
            side=side,
            entry_spot=spot,
            entry_premium=None,
            stop_premium=None,
            high_premium=None,
            low_premium=None,
            next_add_level=1,
            adds_done=0,
            lots_open=cfg.lots_per_add,
        )
        self.mode = Mode.LADDER_CALL if side == "CALL" else Mode.LADDER_PUT
        self._setup = None
        self._started_once = True

    def _handle_stop_hit(self, *, exit_premium: float, spot_ltp: float, cfg: EngineConfig) -> list[Action]:
        ladder = self._ladder
        if ladder is None:
            return []

        entry_prem = ladder.entry_premium
        if entry_prem is None:
            return []
        pnl_points = (exit_premium - float(entry_prem)) if self._kind == "BUY" else (float(entry_prem) - exit_premium)
        is_loss = pnl_points <= 0

        new_loss_count = self.loss_count + (1 if is_loss else 0)
        if new_loss_count >= cfg.max_losses_per_day:
            self.loss_count = new_loss_count
            out = [
                CloseLadder(
                    side=ladder.side, spot=float(spot_ltp), lots_open=ladder.lots_open, reason="stop_max_losses", flip_to=None
                )
            ]
            self._close_and_lock_day(reason="max_losses")
            return out

        self.loss_count = new_loss_count
        flip_to: LadderSide = "PUT" if ladder.side == "CALL" else "CALL"
        out = [
            CloseLadder(side=ladder.side, spot=float(spot_ltp), lots_open=ladder.lots_open, reason="stop_flip", flip_to=flip_to)
        ]
        self._open_ladder(side=flip_to, spot=float(spot_ltp), cfg=cfg)
        return out

    def manual_square_off_and_flip(self, *, spot: float, cfg: EngineConfig) -> list[Action]:
        """
        Manual square-off of current ladder and immediate flip to the opposite side.
        Applies to both BUY and SELL engines.
        """
        ladder = self._ladder
        if ladder is None or self.day_locked:
            return []

        flip_to: LadderSide = "PUT" if ladder.side == "CALL" else "CALL"
        out = [
            CloseLadder(
                side=ladder.side,
                spot=float(spot),
                lots_open=int(ladder.lots_open),
                reason="manual_flip",
                flip_to=flip_to,
            )
        ]
        self._open_ladder(side=flip_to, spot=float(spot), cfg=cfg)
        return out

    def manual_square_off(self, *, spot: float) -> list[Action]:
        """
        Manual square-off of the current ladder (no flip).

        Intended to be used with an engine stop; we reset local ladder state
        back to breakout monitoring.
        """
        ladder = self._ladder
        if ladder is None or self.day_locked:
            return []

        out = [
            CloseLadder(
                side=ladder.side,
                spot=float(spot),
                lots_open=int(ladder.lots_open),
                reason="manual_squareoff",
                flip_to=None,
            )
        ]
        self.mode = Mode.WAITING_BREAKOUT
        self._ladder = None
        self._setup = None
        self._candles.clear()
        self._started_once = False
        return out

    def _close_and_lock_day(self, *, reason: DayLockReason) -> None:
        self._ladder = None
        self.day_locked = True
        self.day_lock_reason = reason
        self.mode = Mode.DAY_LOCKED
