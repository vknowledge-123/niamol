from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Deque, Literal, Optional, TypeAlias

from app.runtime.settings import EngineConfig
from app.services.market.models import Candle, SpotTick


LadderSide = Literal["CALL", "PUT"]
DayLockReason = Literal["target", "max_losses", "manual_squareoff"]


class Mode(str, Enum):
    WAITING_BREAKOUT = "waiting_breakout"
    WAITING_MANUAL = "waiting_manual"
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
    adds_done: int
    lots_open: int
    # Candle-based (spot) trailing stop anchor (1m candles in IST).
    candle_stop_spot: Optional[float] = None
    # Last seen option premium LTP (best-effort; used for candle stop loss-count logic).
    last_premium: Optional[float] = None
    # Candle-add reference closes (previous green close for CALL, previous red close for PUT).
    last_green_close: Optional[float] = None
    last_red_close: Optional[float] = None


class StrategyEngine:
    def __init__(self, *, kind: str = "BUY") -> None:
        self._kind = "SELL" if str(kind).upper() == "SELL" else "BUY"
        self.mode: Mode = Mode.WAITING_BREAKOUT
        # Store a small rolling window; actual consecutive requirement is taken from config.
        self._candles: Deque[Candle] = deque(maxlen=20)
        self._setup: Optional[BreakoutSetup] = None
        self._ladder: Optional[LadderState] = None
        # After a stop/TSL hit (non day-lock), the engine can pause and wait for a manual decision
        # to either continue the same ladder or flip to the opposite side.
        self._pending_manual_side: Optional[LadderSide] = None

        self.loss_count: int = 0
        self.day_locked: bool = False
        self.day_lock_reason: Optional[DayLockReason] = None
        self._started_once: bool = False
        self.last_tick: Optional[SpotTick] = None
        self._last_1m_candle: Optional[Candle] = None

    _last_cfg_pcl_trailing: Optional[bool] = None
    _last_cfg_pdh_trailing: Optional[bool] = None
    _last_cfg_candle_add_min_points: Optional[float] = None

    @classmethod
    def for_engine_kind(cls, *, kind: str) -> "StrategyEngine":
        """
        Factory hook for engine-kind specific behavior.

        NOTE: Candle criteria are enforced to *start* a ladder (via on_candle/setup).
        After a ladder is running, behavior on TSL/SL hit is controlled by config:
        - `full_automation`: auto-flip immediately (no candle re-check).
        - `trade_direction_continue`: auto re-enter same side (no candle re-check).
        - else: pause in WAITING_MANUAL until user chooses.
        """
        return cls(kind=kind)

    def prime_1m_candle(self, candle: Candle) -> None:
        """
        Seed the latest completed 1-minute candle (IST).

        Used by background market-data services so candle-based trailing can be
        active immediately when an engine starts mid-candle.
        """
        self._last_1m_candle = candle

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
        ladder = self._ladder
        return None if ladder is None else ladder.candle_stop_spot

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
        # When candle trailing is enabled for the active ladder, premium-based adds are disabled.
        if ladder.side == "CALL" and bool(self._last_cfg_pcl_trailing):
            return None
        if ladder.side == "PUT" and bool(self._last_cfg_pdh_trailing):
            return None
        initial_tsl = self._last_cfg_initial_tsl
        seq_diff = self._last_cfg_seq_diff
        if initial_tsl is None or seq_diff is None:
            return None
        max_adds = self._last_cfg_max_adds
        if max_adds is not None and max_adds > 0 and int(ladder.adds_done) >= int(max_adds):
            return None
        entry = float(ladder.entry_premium)
        next_n = int(ladder.adds_done) + 1
        if next_n <= 0:
            return None
        initial_tsl = float(initial_tsl)
        seq_diff = float(seq_diff)
        if initial_tsl <= 0:
            return None
        if seq_diff < 0:
            seq_diff = 0.0
        cum = (next_n * initial_tsl) + (seq_diff * (next_n * (next_n + 1) / 2.0))
        return (entry + cum) if self._kind == "BUY" else (entry - cum)

    _last_cfg_initial_tsl: Optional[float] = None
    _last_cfg_seq_diff: Optional[float] = None
    _last_cfg_max_adds: Optional[int] = None

    @staticmethod
    def _candle_add_min_points(cfg: EngineConfig) -> float:
        v = float(getattr(cfg, "candle_add_min_points", 5.0) or 0.0)
        return 0.0 if v < 0 else v

    @staticmethod
    def _candle_stop_buffer_points(cfg: EngineConfig) -> float:
        v = float(getattr(cfg, "candle_stop_buffer_points", 2.5) or 0.0)
        return 0.0 if v < 0 else v

    @staticmethod
    def _candle_trailing_enabled_for(side: LadderSide, cfg: EngineConfig) -> bool:
        if side == "CALL":
            return bool(getattr(cfg, "pcl_trailing", False))
        return bool(getattr(cfg, "pdh_trailing", False))

    def on_1m_candle(self, candle: Candle, cfg: EngineConfig) -> list[Action]:
        """
        1-minute IST candle close hook for candle-based trailing stop + candle-only adds.

        - CALL ladder: trail stop to previous candle LOW (PCL), tighten only upwards.
        - PUT ladder:  trail stop to previous candle HIGH (PDH), tighten only downwards.
        - Adds: only on candle close, with color + close-delta filters (spot points).
        """
        self._last_1m_candle = candle
        self._last_cfg_pcl_trailing = bool(getattr(cfg, "pcl_trailing", False))
        self._last_cfg_pdh_trailing = bool(getattr(cfg, "pdh_trailing", False))
        self._last_cfg_candle_add_min_points = self._candle_add_min_points(cfg)

        if self.day_locked or self._pending_manual_side is not None:
            return []

        ladder = self._ladder
        if ladder is None:
            return []
        if not self._candle_trailing_enabled_for(ladder.side, cfg):
            return []

        # --- Trailing stop (spot) -------------------------------------------------
        buf = self._candle_stop_buffer_points(cfg)
        raw = (float(candle.low) - float(buf)) if ladder.side == "CALL" else (float(candle.high) + float(buf))
        prev = ladder.candle_stop_spot
        if prev is None:
            ladder.candle_stop_spot = raw
        else:
            ladder.candle_stop_spot = max(float(prev), raw) if ladder.side == "CALL" else min(float(prev), raw)

        # --- Adds (spot candle close only) ---------------------------------------
        max_adds = int(getattr(cfg, "max_adds", 0) or 0)
        if max_adds > 0 and int(ladder.adds_done) >= max_adds:
            return []

        min_pts = float(self._last_cfg_candle_add_min_points or 0.0)
        should_add = False

        if ladder.side == "CALL":
            if candle.green:
                prev_green = ladder.last_green_close
                if prev_green is None or float(candle.close) >= float(prev_green) + min_pts - 1e-12:
                    should_add = True
                ladder.last_green_close = float(candle.close)
        else:
            if candle.red:
                prev_red = ladder.last_red_close
                if prev_red is None or float(candle.close) <= float(prev_red) - min_pts + 1e-12:
                    should_add = True
                ladder.last_red_close = float(candle.close)

        if not should_add:
            return []

        ladder.adds_done += 1
        ladder.lots_open += int(cfg.lots_per_add)
        return [AddLot(side=ladder.side, spot=float(candle.close), levels=1)]

    def apply_execution_entry_premium(self, *, premium: float, cfg: EngineConfig) -> bool:
        """
        Set ladder entry premium from broker execution (avg traded price).

        This is preferred over using the first option LTP tick as "entry".
        """
        if self.day_locked:
            return False
        ladder = self._ladder
        if ladder is None:
            return False

        # Avoid changing the anchor after scaling in; it would invalidate add/TSL levels.
        if ladder.entry_premium is not None and int(ladder.adds_done) > 0:
            return False

        prem = float(premium)
        self._last_cfg_initial_tsl = float(getattr(cfg, "initial_tsl_points", 0.0) or 0.0)
        self._last_cfg_seq_diff = float(getattr(cfg, "sequence_tsl_diff_points", 0.0) or 0.0)
        self._last_cfg_max_adds = int(getattr(cfg, "max_adds", 0) or 0)
        ladder.entry_premium = prem
        ladder.high_premium = prem
        ladder.low_premium = prem
        ladder.last_premium = prem
        if self._candle_trailing_enabled_for(ladder.side, cfg):
            ladder.stop_premium = None
            if ladder.candle_stop_spot is None and self._last_1m_candle is not None:
                buf = self._candle_stop_buffer_points(cfg)
                ladder.candle_stop_spot = (
                    (float(self._last_1m_candle.low) - float(buf))
                    if ladder.side == "CALL"
                    else (float(self._last_1m_candle.high) + float(buf))
                )
        else:
            ladder.stop_premium = self._recompute_stop_premium(
                entry=float(prem),
                high=float(prem),
                low=float(prem),
                adds_done=int(ladder.adds_done),
                cfg=cfg,
            )
        return True

    def reset_day(self) -> None:
        self.mode = Mode.WAITING_BREAKOUT
        self._candles.clear()
        self._setup = None
        self._ladder = None
        self._pending_manual_side = None
        self.loss_count = 0
        self.day_locked = False
        self.day_lock_reason = None
        self._started_once = False
        self._last_1m_candle = None

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
        if self.day_locked or self._started_once or self._pending_manual_side is not None:
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
        self._last_cfg_initial_tsl = float(getattr(cfg, "initial_tsl_points", 0.0) or 0.0)
        self._last_cfg_seq_diff = float(getattr(cfg, "sequence_tsl_diff_points", 0.0) or 0.0)
        self._last_cfg_max_adds = int(getattr(cfg, "max_adds", 0) or 0)
        self._last_cfg_pcl_trailing = bool(getattr(cfg, "pcl_trailing", False))
        self._last_cfg_pdh_trailing = bool(getattr(cfg, "pdh_trailing", False))
        self._last_cfg_candle_add_min_points = self._candle_add_min_points(cfg)

        if self.day_locked:
            self.mode = Mode.DAY_LOCKED
            return []

        actions: list[Action] = []

        if self._pending_manual_side is not None:
            # A stop/TSL was hit; wait for explicit user choice before opening any new ladder.
            self.mode = Mode.WAITING_MANUAL
            return []

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
        if self._candle_trailing_enabled_for(ladder.side, cfg) and ladder.candle_stop_spot is not None:
            stop_spot = float(ladder.candle_stop_spot)
            spot = float(tick.ltp)
            stop_hit = (spot <= stop_spot) if ladder.side == "CALL" else (spot >= stop_spot)
            if stop_hit:
                exit_prem = float(ladder.last_premium or ladder.entry_premium or 0.0)
                return self._handle_stop_hit(exit_premium=exit_prem, spot_ltp=spot, cfg=cfg)
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
            self.mode = Mode.WAITING_MANUAL if self._pending_manual_side is not None else Mode.WAITING_BREAKOUT
            return []

        self.mode = Mode.LADDER_CALL if ladder.side == "CALL" else Mode.LADDER_PUT

        prem = float(premium_ltp)
        actions: list[Action] = []
        self._last_cfg_initial_tsl = float(getattr(cfg, "initial_tsl_points", 0.0) or 0.0)
        self._last_cfg_seq_diff = float(getattr(cfg, "sequence_tsl_diff_points", 0.0) or 0.0)
        self._last_cfg_max_adds = int(getattr(cfg, "max_adds", 0) or 0)
        self._last_cfg_pcl_trailing = bool(getattr(cfg, "pcl_trailing", False))
        self._last_cfg_pdh_trailing = bool(getattr(cfg, "pdh_trailing", False))
        self._last_cfg_candle_add_min_points = self._candle_add_min_points(cfg)

        ladder.last_premium = prem

        if ladder.entry_premium is None:
            ladder.entry_premium = prem
            ladder.high_premium = prem
            ladder.low_premium = prem
            ladder.adds_done = 0
            if self._candle_trailing_enabled_for(ladder.side, cfg):
                ladder.stop_premium = None
                if ladder.candle_stop_spot is None and self._last_1m_candle is not None:
                    buf = self._candle_stop_buffer_points(cfg)
                    ladder.candle_stop_spot = (
                        (float(self._last_1m_candle.low) - float(buf))
                        if ladder.side == "CALL"
                        else (float(self._last_1m_candle.high) + float(buf))
                    )
            else:
                ladder.stop_premium = self._recompute_stop_premium(
                    entry=float(prem),
                    high=float(prem),
                    low=float(prem),
                    adds_done=int(ladder.adds_done),
                    cfg=cfg,
                )
            return []

        entry = float(ladder.entry_premium)
        favorable = (prem - entry) if self._kind == "BUY" else (entry - prem)

        if favorable >= float(cfg.target_points):
            actions.append(CloseLadder(side=ladder.side, spot=float(spot_ltp), lots_open=ladder.lots_open, reason="target", flip_to=None))
            self._close_and_lock_day(reason="target")
            return actions

        # Update high/low before recomputing stop with current config.
        if self._kind == "BUY":
            if ladder.high_premium is None or prem > float(ladder.high_premium):
                ladder.high_premium = prem
        else:
            if ladder.low_premium is None or prem < float(ladder.low_premium):
                ladder.low_premium = prem

        if ladder.high_premium is None:
            ladder.high_premium = float(entry)
        if ladder.low_premium is None:
            ladder.low_premium = float(entry)

        if self._candle_trailing_enabled_for(ladder.side, cfg):
            ladder.stop_premium = None
            return actions

        # Evaluate adds sequentially using the favorable extreme (BUY: high, SELL: low).
        max_adds = int(getattr(cfg, "max_adds", 0) or 0)
        initial_tsl = float(getattr(cfg, "initial_tsl_points", 0.0) or 0.0)
        seq_diff = float(getattr(cfg, "sequence_tsl_diff_points", 0.0) or 0.0)
        if initial_tsl > 0 and seq_diff < 0:
            seq_diff = 0.0

        def _next_threshold(entry_p: float, n: int) -> Optional[float]:
            if n <= 0 or initial_tsl <= 0:
                return None
            cum = (n * initial_tsl) + (seq_diff * (n * (n + 1) / 2.0))
            return (entry_p + cum) if self._kind == "BUY" else (entry_p - cum)

        extreme = float(ladder.high_premium) if self._kind == "BUY" else float(ladder.low_premium)
        to_add = 0
        while True:
            if max_adds > 0 and int(ladder.adds_done) + int(to_add) >= max_adds:
                break
            next_n = int(ladder.adds_done) + int(to_add) + 1
            thresh = _next_threshold(entry, next_n)
            if thresh is None:
                break
            if self._kind == "BUY":
                if extreme + 1e-12 < float(thresh):
                    break
            else:
                if extreme - 1e-12 > float(thresh):
                    break
            to_add += 1

        if to_add > 0:
            ladder.adds_done += int(to_add)
            ladder.lots_open += int(to_add) * int(cfg.lots_per_add)
            actions.append(AddLot(side=ladder.side, spot=float(spot_ltp), levels=int(to_add)))

        ladder.stop_premium = self._recompute_stop_premium(
            entry=float(entry),
            high=float(ladder.high_premium),
            low=float(ladder.low_premium),
            adds_done=int(ladder.adds_done),
            cfg=cfg,
        )

        stop = ladder.stop_premium
        if stop is not None:
            if self._kind == "BUY" and prem <= float(stop):
                actions.extend(self._handle_stop_hit(exit_premium=prem, spot_ltp=float(spot_ltp), cfg=cfg))
            elif self._kind == "SELL" and prem >= float(stop):
                actions.extend(self._handle_stop_hit(exit_premium=prem, spot_ltp=float(spot_ltp), cfg=cfg))

        return actions

    def apply_live_config(self, cfg: EngineConfig) -> bool:
        """
        Apply config changes immediately to any open ladder state (best-effort).

        This updates cached cfg fields and recomputes the current stop premium from
        the existing entry/high/low anchors, even if no new option tick arrives.
        """
        self._last_cfg_initial_tsl = float(getattr(cfg, "initial_tsl_points", 0.0) or 0.0)
        self._last_cfg_seq_diff = float(getattr(cfg, "sequence_tsl_diff_points", 0.0) or 0.0)
        self._last_cfg_max_adds = int(getattr(cfg, "max_adds", 0) or 0)
        self._last_cfg_pcl_trailing = bool(getattr(cfg, "pcl_trailing", False))
        self._last_cfg_pdh_trailing = bool(getattr(cfg, "pdh_trailing", False))
        self._last_cfg_candle_add_min_points = self._candle_add_min_points(cfg)

        ladder = self._ladder
        if ladder is None or ladder.entry_premium is None:
            return False
        if ladder.high_premium is None:
            ladder.high_premium = float(ladder.entry_premium)
        if ladder.low_premium is None:
            ladder.low_premium = float(ladder.entry_premium)
        prev = ladder.stop_premium
        if self._candle_trailing_enabled_for(ladder.side, cfg):
            ladder.stop_premium = None
        else:
            ladder.stop_premium = self._recompute_stop_premium(
                entry=float(ladder.entry_premium),
                high=float(ladder.high_premium),
                low=float(ladder.low_premium),
                adds_done=int(ladder.adds_done),
                cfg=cfg,
            )
        if prev is None:
            return True
        if ladder.stop_premium is None:
            return True
        return abs(float(prev) - float(ladder.stop_premium)) > 1e-12

    def _open_ladder(self, *, side: LadderSide, spot: float, cfg: EngineConfig) -> None:
        buf = self._candle_stop_buffer_points(cfg)
        self._ladder = LadderState(
            side=side,
            entry_spot=spot,
            entry_premium=None,
            stop_premium=None,
            high_premium=None,
            low_premium=None,
            adds_done=0,
            lots_open=cfg.lots_per_add,
            candle_stop_spot=(
                None
                if (not self._candle_trailing_enabled_for(side, cfg) or self._last_1m_candle is None)
                else (
                    (float(self._last_1m_candle.low) - float(buf))
                    if side == "CALL"
                    else (float(self._last_1m_candle.high) + float(buf))
                )
            ),
            last_premium=None,
            last_green_close=None,
            last_red_close=None,
        )
        self.mode = Mode.LADDER_CALL if side == "CALL" else Mode.LADDER_PUT
        self._setup = None
        self._started_once = True

    def _recompute_stop_premium(self, *, entry: float, high: float, low: float, adds_done: int, cfg: EngineConfig) -> float:
        """
        Continuous trailing stop with "bounce-back" on adds.

        Trail distance is dynamic:
        - distance = initial_tsl_points + adds_done * sequence_tsl_diff_points
        """
        initial_tsl = float(getattr(cfg, "initial_tsl_points", 0.0) or 0.0)
        seq_diff = float(getattr(cfg, "sequence_tsl_diff_points", 0.0) or 0.0)
        if initial_tsl < 0:
            initial_tsl = 0.0
        if seq_diff < 0:
            seq_diff = 0.0
        n_adds = max(0, int(adds_done))
        trail_distance = float(initial_tsl) + (float(n_adds) * float(seq_diff))

        if self._kind == "BUY":
            return float(high) - trail_distance
        return float(low) + trail_distance

    def _handle_stop_hit(self, *, exit_premium: float, spot_ltp: float, cfg: EngineConfig) -> list[Action]:
        ladder = self._ladder
        if ladder is None:
            return []

        entry_prem = ladder.entry_premium
        if entry_prem is None:
            is_loss = True
        else:
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

        if bool(getattr(cfg, "full_automation", False)):
            flip_to: LadderSide = "PUT" if ladder.side == "CALL" else "CALL"
            out = [
                CloseLadder(
                    side=ladder.side,
                    spot=float(spot_ltp),
                    lots_open=ladder.lots_open,
                    reason="stop_flip",
                    flip_to=flip_to,
                )
            ]
            self._pending_manual_side = None
            self._open_ladder(side=flip_to, spot=float(spot_ltp), cfg=cfg)
            return out

        if bool(getattr(cfg, "trade_direction_continue", False)):
            out = [
                CloseLadder(
                    side=ladder.side,
                    spot=float(spot_ltp),
                    lots_open=ladder.lots_open,
                    reason="stop_continue",
                    flip_to=None,
                ),
                OpenLadder(side=ladder.side, spot=float(spot_ltp)),
            ]
            self._open_ladder(side=ladder.side, spot=float(spot_ltp), cfg=cfg)
            return out

        # Default: do NOT auto-flip. Close and wait for a manual decision.
        self._pending_manual_side = ladder.side
        self._ladder = None
        self._setup = None
        self.mode = Mode.WAITING_MANUAL
        return [CloseLadder(side=ladder.side, spot=float(spot_ltp), lots_open=ladder.lots_open, reason="stop_manual", flip_to=None)]

    def has_pending_manual_decision(self) -> bool:
        return self._pending_manual_side is not None

    def manual_continue_same(self, *, spot: float, cfg: EngineConfig) -> list[Action]:
        """
        After a stop/TSL hit (WAITING_MANUAL), restart the same ladder side explicitly.
        """
        if self.day_locked or self._ladder is not None:
            return []
        side = self._pending_manual_side
        if side is None:
            return []
        self._pending_manual_side = None
        out: list[Action] = [OpenLadder(side=side, spot=float(spot))]
        self._open_ladder(side=side, spot=float(spot), cfg=cfg)
        return out

    def manual_flip_opposite(self, *, spot: float, cfg: EngineConfig) -> list[Action]:
        """
        After a stop/TSL hit (WAITING_MANUAL), flip to the opposite ladder side explicitly.
        """
        if self.day_locked or self._ladder is not None:
            return []
        side = self._pending_manual_side
        if side is None:
            return []
        flip_to: LadderSide = "PUT" if side == "CALL" else "CALL"
        self._pending_manual_side = None
        out: list[Action] = [OpenLadder(side=flip_to, spot=float(spot))]
        self._open_ladder(side=flip_to, spot=float(spot), cfg=cfg)
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
        Manual square-off of the current ladder (no flip), then day-lock.
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
        self._close_and_lock_day(reason="manual_squareoff")
        return out

    def _close_and_lock_day(self, *, reason: DayLockReason) -> None:
        self._ladder = None
        self.day_locked = True
        self.day_lock_reason = reason
        self.mode = Mode.DAY_LOCKED
