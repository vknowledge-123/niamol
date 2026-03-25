from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from zoneinfo import ZoneInfo

from app.runtime.instruments import InstrumentStore, OptionContract
from app.runtime.settings import EngineConfig, EngineConfigStore, EngineStatus
from app.services.engine.latency import LatencyRecorder
from app.services.candles.aggregator import CandleAggregator
from app.services.dhan.feed import DhanMarketFeed
from app.services.dhan.rest import DhanRest
from app.services.engine.strategy import AddLot, CloseLadder, OpenLadder, StrategyEngine
from app.services.market.models import SpotTick


log = logging.getLogger("niftyalgo.engine")
IST = ZoneInfo("Asia/Kolkata")


@dataclass(slots=True)
class _OrderBatch:
    # A batch is executed sequentially by the order worker.
    ops: list[tuple[str, str, int, str]]  # (txn, security_id, quantity, tag)
    enqueued_ns: int
    done: Optional[asyncio.Future[None]] = None


RunMode = Literal["LIVE", "SIM"]
EngineKind = Literal["BUY", "SELL"]
Underlying = Literal["NIFTY", "BANKNIFTY"]


@dataclass(slots=True)
class _SimFill:
    ts: datetime
    spot: float
    qty: int
    premium: Optional[float]


@dataclass(slots=True)
class _SimTrade:
    id: int
    side: str  # display side (e.g. CALL/PUT or CALL_SELL/PUT_SELL)
    strategy_side: str  # CALL/PUT
    trade_side: str  # CALL/PUT (actual option type traded)
    kind: EngineKind  # BUY/SELL at the time this trade was opened
    contract: OptionContract
    fills: list[_SimFill]
    exit_ts: Optional[datetime] = None
    exit_spot: Optional[float] = None
    exit_premium: Optional[float] = None
    exit_reason: Optional[str] = None
    flip_to: Optional[str] = None


class EngineController:
    def __init__(
        self,
        config_store: EngineConfigStore,
        instruments: InstrumentStore,
        *,
        kind: EngineKind = "BUY",
        underlying: Underlying = "NIFTY",
        spot_candles: object | None = None,
    ) -> None:
        self._cfg_store = config_store
        self._instruments = instruments
        self._kind: EngineKind = kind
        self._underlying: Underlying = "BANKNIFTY" if str(underlying).upper() == "BANKNIFTY" else "NIFTY"
        self._spot_candles = spot_candles
        self._last_seen_1m_end: Optional[datetime] = None

        self._engine = StrategyEngine.for_engine_kind(kind=self._kind)
        self._running = False
        self._lock = asyncio.Lock()
        self._market_lock = asyncio.Lock()

        self._market_task: Optional[asyncio.Task] = None
        self._orders_task: Optional[asyncio.Task] = None
        self._orders_q: asyncio.Queue[_OrderBatch] = asyncio.Queue(maxsize=2048)

        self._feed: Optional[DhanMarketFeed] = None
        self._rest: Optional[DhanRest] = None

        self._active_contract: Optional[OptionContract] = None
        self._option_ltps: dict[str, float] = {}
        self._last_error: Optional[str] = None
        self._feed_error: Optional[str] = None
        self._last_ist_date: Optional[str] = None
        self._spot_security_id: Optional[str] = None
        self._run_mode: Optional[RunMode] = None
        self._lat = LatencyRecorder(sample_every_n=10, maxlen=4096)
        self._last_recv_ns: Optional[int] = None

        # Simulation state (in-memory)
        self._sim_seq: int = 0
        self._sim_trades: list[_SimTrade] = []
        self._sim_active: Optional[_SimTrade] = None

        # Live MTM tracking (best-effort; based on option LTP ticks, not broker fills).
        self._mtm_active: Optional[_SimTrade] = None
        # Broker execution updates may arrive out-of-band; keep a small pending buffer keyed by security_id.
        self._pending_exec_entry_premiums: dict[str, float] = {}

        # Cached config override to allow simulation even when config.trading_enabled is false.
        self._sim_cfg_ver: int = -1
        self._sim_cfg_override: Optional[EngineConfig] = None

        # Premium-driven ladder management is handled by StrategyEngine; controller keeps no P&L exit rules.

    @property
    def kind(self) -> EngineKind:
        return self._kind

    @property
    def position(self) -> str:
        return "LONG" if self._kind == "BUY" else "SHORT"

    def _pnl_sign(self) -> int:
        # Long premium: exit - entry. Short premium: entry - exit.
        return 1 if self._kind == "BUY" else -1

    @staticmethod
    def _pnl_sign_for_kind(kind: EngineKind) -> int:
        return 1 if str(kind).upper() == "BUY" else -1

    def _map_strategy_to_trade_side(self, strategy_side: str) -> str:
        # BUY engine: trade CALL on CALL ladder, PUT on PUT ladder.
        # SELL engine: trade PUT_SELL on CALL ladder, CALL_SELL on PUT ladder.
        if self._kind == "BUY":
            return strategy_side
        return "PUT" if strategy_side == "CALL" else "CALL"

    def _display_side(self, strategy_side: str) -> str:
        trade_side = self._map_strategy_to_trade_side(strategy_side)
        if self._kind == "SELL":
            return f"{trade_side}_SELL"
        return trade_side

    async def start(self, mode: RunMode = "LIVE") -> None:
        async with self._lock:
            if self._running:
                if self._run_mode == mode:
                    return
                raise RuntimeError(f"Engine already running in mode={self._run_mode}. Stop it before starting {mode}.")

            try:
                cfg = await self._cfg_store.get()
                if mode == "LIVE" and not cfg.trading_enabled:
                    msg = "Set trading_enabled=true in config before starting (or use simulation)."
                    self._last_error = msg
                    raise RuntimeError(msg)
                if not cfg.client_id or not cfg.access_token:
                    msg = "Set Dhan client_id and access_token in config before starting."
                    self._last_error = msg
                    raise RuntimeError(msg)
                if not self._instruments.loaded:
                    msg = "Instrument master not loaded. Call POST /api/instruments/refresh first."
                    self._last_error = msg
                    raise RuntimeError(msg)

                spot_sid = await self._instruments.spot_security_id(symbol=self._underlying, default=cfg.spot_security_id)
                self._spot_security_id = str(spot_sid)

                # Clear previous non-fatal errors for this new run; keep any warnings we set during startup.
                self._last_error = None

                self._feed = DhanMarketFeed(cfg.client_id, cfg.access_token, spot_security_id=self._spot_security_id)
                try:
                    await self._feed.connect()
                except Exception as e:
                    # Don't hard-fail engine start on websocket issues; allow reconnect loop to recover.
                    self._feed.notify_ws_error(e)
                    msg = f"Dhan marketfeed websocket connection failed: {e}"
                    self._last_error = msg
                self._rest = None
                if mode == "LIVE":
                    self._rest = DhanRest(cfg.client_id, cfg.access_token)
                    # Simple auth check (raises if token invalid / network issue)
                    try:
                        await asyncio.to_thread(self._rest.client.get_fund_limits)
                    except Exception as e:
                        msg = f"Dhan authentication failed: {e}"
                        self._last_error = msg
                        raise RuntimeError(msg) from e

                self._engine.reset_day()
                self._last_seen_1m_end = None
                if self._spot_candles is not None:
                    try:
                        c = getattr(self._spot_candles, "last_completed_1m", None)
                        if callable(c):
                            last_1m = c(self._underlying)
                            if last_1m is not None:
                                self._engine.prime_1m_candle(last_1m)
                    except Exception:
                        # Best-effort; engines can still build candles locally.
                        pass
                self._active_contract = None
                self._option_ltps.clear()
                self._mtm_active = None
                self._run_mode = mode
                if mode == "SIM":
                    self._sim_seq = 0
                    self._sim_trades.clear()
                    self._sim_active = None
                    self._mtm_active = None

                self._running = True
                self._lat.inc("engine_start")

                self._orders_task = None
                if mode == "LIVE":
                    self._orders_task = asyncio.create_task(self._orders_worker(), name="orders_worker")
                self._market_task = asyncio.create_task(self._market_loop(), name="market_loop")
            except Exception as e:
                # Best-effort cleanup if partial init happened.
                self._running = False
                if self._feed:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await self._feed.disconnect()
                    self._feed = None
                self._rest = None
                if self._last_error is None:
                    self._last_error = str(e)
                if isinstance(e, RuntimeError):
                    raise
                raise RuntimeError(self._last_error) from e

    async def stop(self) -> None:
        current = asyncio.current_task()
        async with self._lock:
            self._running = False
            self._run_mode = None
            self._feed_error = None


        if self._market_task:
            if self._market_task is current:
                # Avoid awaiting on ourselves (can happen on internal stops triggered by the market loop).
                self._market_task = None
            else:
                self._market_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._market_task
                self._market_task = None

        if self._orders_task:
            if self._orders_task is current:
                self._orders_task = None
            else:
                self._orders_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._orders_task
                self._orders_task = None

        if self._feed:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._feed.disconnect()
            self._feed = None

        self._active_contract = None
        self._option_ltps.clear()
        self._mtm_active = None
        self._lat.inc("engine_stop")

    async def status(self) -> EngineStatus:
        cfg = self._cfg_store.current()
        tick = self._engine.last_tick
        lots_open = self._engine.lots_open
        adds_done = self._engine.adds_done
        active_ladder = self._engine.active_side
        active_contract = self._active_contract
        active_ltp = None
        if active_contract is not None:
            active_ltp = self._option_ltps.get(active_contract.security_id)

        active_qty = None
        if active_contract is not None:
            active_qty = int(max(1, active_contract.lot_size) * int(lots_open))

        err = self._last_error
        if self._feed_error:
            err = f"{err} | {self._feed_error}" if err else self._feed_error
        return EngineStatus(
            running=self._running,
            engine_kind=self._kind,
            underlying=self._underlying,
            position=self.position,
            trading_enabled=cfg.trading_enabled,
            mode=self._engine.mode.value,
            active_ladder=None if active_ladder is None else self._display_side(active_ladder),
            spot_ltp=None if tick is None else tick.ltp,
            entry_spot=self._engine.entry_spot,
            stop_spot=self._engine.stop_spot,
            next_add_spot=self._engine.next_add_spot,
            lots_open=lots_open,
            adds_done=int(adds_done),
            max_adds=int(getattr(cfg, "max_adds", 0) or 0),
            loss_count=self._engine.loss_count,
            day_locked=self._engine.day_locked,
            active_contract_symbol=None if active_contract is None else active_contract.trading_symbol,
            active_contract_security_id=None if active_contract is None else active_contract.security_id,
            active_option_ltp=None if active_ltp is None else float(active_ltp),
            active_contract_expiry=None if active_contract is None else active_contract.expiry.isoformat(),
            active_contract_strike=None if active_contract is None else int(active_contract.strike),
            active_contract_option_type=None if active_contract is None else str(active_contract.option_type),
            active_contract_lot_size=None if active_contract is None else int(active_contract.lot_size),
            active_qty=active_qty,
            contract_kind=str(getattr(cfg, "contract_kind", "WEEKLY") or "WEEKLY"),
            weekly_expiry=str(getattr(cfg, "weekly_expiry", "CURRENT") or "CURRENT"),
            monthly_expiry_offset=int(getattr(cfg, "monthly_expiry_offset", 0) or 0),
            entry_premium=self._engine.entry_premium,
            stop_premium=self._engine.stop_premium,
            next_add_premium=self._engine.next_add_premium,
            last_error=err,
        )

    async def on_config_updated(self, cfg: EngineConfig) -> None:
        """
        Best-effort: apply config updates immediately to any open ladder.

        This recomputes stop levels even if no new option tick arrives, and if we
        have a cached option LTP it also re-evaluates add/target/stop immediately.
        """
        if not self._running:
            return

        cfg_eng = self._cfg_for_engine(cfg)
        async with self._market_lock:
            self._engine.maybe_unlock_day(cfg_eng)
            self._engine.apply_live_config(cfg_eng)

            # If we were waiting for manual action after a stop, allow config toggles to resolve it immediately.
            tick = self._engine.last_tick
            if tick is not None and bool(getattr(cfg_eng, "trading_enabled", False)) and self._engine.has_pending_manual_decision():
                now = datetime.now(tz=IST)
                actions = []
                if bool(getattr(cfg_eng, "full_automation", False)):
                    actions = self._engine.manual_flip_opposite(spot=float(tick.ltp), cfg=cfg_eng)
                elif bool(getattr(cfg_eng, "trade_direction_continue", False)):
                    actions = self._engine.manual_continue_same(spot=float(tick.ltp), cfg=cfg_eng)
                if actions:
                    await self._handle_actions(actions, spot=float(tick.ltp), cfg=cfg_eng, now=now)

            active_contract = self._active_contract
            last_spot = self._engine.last_tick
            if active_contract is None or last_spot is None:
                return
            ltp = self._option_ltps.get(active_contract.security_id)
            if ltp is None:
                return
            now = datetime.now(tz=IST)
            actions = self._engine.on_option_tick(premium_ltp=float(ltp), spot_ltp=float(last_spot.ltp), cfg=cfg_eng)
            await self._handle_actions(actions, spot=float(last_spot.ltp), cfg=cfg_eng, now=now)

    async def unlock_day(self) -> EngineStatus:
        """
        Clears the day-lock (including target lock) without stopping the engine.
        Intended for discretionary "trade further" workflows after a target is hit.
        """
        if not self._running:
            raise RuntimeError("Engine not running.")

        async with self._market_lock:
            if not self._engine.day_locked:
                raise RuntimeError("Engine is not day-locked.")
            self._engine.force_unlock_day()
            # Best-effort cleanup of any stale local state after a close+lock cycle.
            self._active_contract = None
            self._option_ltps.clear()

        return await self.status()

    async def square_off_and_flip(self) -> EngineStatus:
        """
        Manual square-off for the currently running ladder and immediate flip to the opposite side.
        """
        if not self._running:
            raise RuntimeError("Engine not running.")
        tick = self._engine.last_tick
        if tick is None:
            raise RuntimeError("No spot tick received yet; cannot square-off.")

        cfg = self._cfg_store.current()
        cfg_eng = self._cfg_for_engine(cfg)
        now = datetime.now(tz=IST)

        async with self._market_lock:
            actions = self._engine.manual_square_off_and_flip(spot=float(tick.ltp), cfg=cfg_eng)
            if not actions:
                raise RuntimeError("No active ladder to square-off.")
            await self._handle_actions(actions, spot=float(tick.ltp), cfg=cfg_eng, now=now)

        return await self.status()

    async def flip_opposite_after_stop(self) -> EngineStatus:
        """
        After a stop/TSL hit (engine waiting for manual decision), open the opposite ladder.
        """
        if not self._running:
            raise RuntimeError("Engine not running.")
        tick = self._engine.last_tick
        if tick is None:
            raise RuntimeError("No spot tick received yet; cannot flip.")

        cfg = self._cfg_store.current()
        cfg_eng = self._cfg_for_engine(cfg)
        if not bool(getattr(cfg_eng, "trading_enabled", False)):
            raise RuntimeError("Trading disabled in config.")
        now = datetime.now(tz=IST)

        async with self._market_lock:
            actions = self._engine.manual_flip_opposite(spot=float(tick.ltp), cfg=cfg_eng)
            if not actions:
                raise RuntimeError("Engine is not waiting for a manual flip/continue decision.")
            await self._handle_actions(actions, spot=float(tick.ltp), cfg=cfg_eng, now=now)

        return await self.status()

    async def continue_same_after_stop(self) -> EngineStatus:
        """
        After a stop/TSL hit (engine waiting for manual decision), re-open the same ladder side.
        """
        if not self._running:
            raise RuntimeError("Engine not running.")
        tick = self._engine.last_tick
        if tick is None:
            raise RuntimeError("No spot tick received yet; cannot continue.")

        cfg = self._cfg_store.current()
        cfg_eng = self._cfg_for_engine(cfg)
        if not bool(getattr(cfg_eng, "trading_enabled", False)):
            raise RuntimeError("Trading disabled in config.")
        now = datetime.now(tz=IST)

        async with self._market_lock:
            actions = self._engine.manual_continue_same(spot=float(tick.ltp), cfg=cfg_eng)
            if not actions:
                raise RuntimeError("Engine is not waiting for a manual flip/continue decision.")
            await self._handle_actions(actions, spot=float(tick.ltp), cfg=cfg_eng, now=now)

        return await self.status()

    async def square_off_and_stop(self) -> EngineStatus:
        """
        Manual square-off for the current ladder (no flip), then day-lock the engine.
        """
        if not self._running:
            raise RuntimeError("Engine not running.")
        tick = self._engine.last_tick
        if tick is None:
            raise RuntimeError("No spot tick received yet; cannot square-off.")

        cfg = self._cfg_store.current()
        cfg_eng = self._cfg_for_engine(cfg)
        now = datetime.now(tz=IST)

        async with self._market_lock:
            actions = self._engine.manual_square_off(spot=float(tick.ltp))
            if not actions:
                raise RuntimeError("No active ladder to square-off.")
            # Ultra-low-latency: enqueue close order(s) and return immediately.
            await self._handle_actions(actions, spot=float(tick.ltp), cfg=cfg_eng, now=now)

        return await self.status()

    def sim_trades(self, limit: int = 200) -> list[dict]:
        out: list[dict] = []
        last_spot = None if self._engine.last_tick is None else float(self._engine.last_tick.ltp)
        window = self._sim_trades[-max(1, int(limit)) :] if self._sim_trades else []
        for tr in window:
            qty_total = sum(f.qty for f in tr.fills)
            entry_fill = tr.fills[0] if tr.fills else None

            entry_premium = None
            if qty_total > 0 and tr.fills and all(f.premium is not None for f in tr.fills):
                entry_premium = sum(f.qty * float(f.premium) for f in tr.fills) / qty_total  # type: ignore[arg-type]

            mark_premium = self._option_ltps.get(tr.contract.security_id)
            is_open = tr.exit_ts is None
            eff_exit_premium = (mark_premium if is_open else tr.exit_premium)
            eff_exit_spot = (last_spot if is_open else tr.exit_spot)

            pnl = None
            pnl_sign = self._pnl_sign_for_kind(tr.kind)
            if eff_exit_premium is not None and tr.fills and all(f.premium is not None for f in tr.fills):
                pnl = pnl_sign * sum(
                    f.qty * (float(eff_exit_premium) - float(f.premium)) for f in tr.fills  # type: ignore[arg-type]
                )

            spot_pnl_points = None
            if entry_fill and eff_exit_spot is not None:
                if tr.strategy_side == "CALL":
                    spot_pnl_points = float(eff_exit_spot) - float(entry_fill.spot)
                else:
                    spot_pnl_points = float(entry_fill.spot) - float(eff_exit_spot)

            out.append(
                {
                    "id": tr.id,
                    "side": tr.side,
                    "strategy_side": tr.strategy_side,
                    "trade_side": tr.trade_side,
                    "position": "LONG" if tr.kind == "BUY" else "SHORT",
                    "symbol": tr.contract.trading_symbol,
                    "security_id": tr.contract.security_id,
                    "qty": qty_total,
                    "entry_ts": None if entry_fill is None else entry_fill.ts.isoformat(),
                    "entry_spot": None if entry_fill is None else entry_fill.spot,
                    "entry_premium": entry_premium,
                    "exit_ts": None if tr.exit_ts is None else tr.exit_ts.isoformat(),
                    "exit_spot": eff_exit_spot,
                    "exit_premium": eff_exit_premium,
                    "exit_reason": None if is_open else tr.exit_reason,
                    "status": "OPEN" if is_open else "CLOSED",
                    "spot_pnl_points": spot_pnl_points,
                    "pnl": pnl,
                }
            )
        return out

    def sim_status(self) -> dict:
        tick = self._engine.last_tick
        active_ladder = self._engine.active_side
        active_contract = self._active_contract
        active_ltp = None
        if active_contract is not None:
            active_ltp = self._option_ltps.get(active_contract.security_id)
        err = self._last_error
        if self._feed_error:
            err = f"{err} | {self._feed_error}" if err else self._feed_error
        return {
            "running": bool(self._running and self._run_mode == "SIM"),
            "engine_kind": self._kind,
            "position": self.position,
            "spot_ltp": None if tick is None else tick.ltp,
            "mode": self._engine.mode.value,
            "active_ladder": None if active_ladder is None else self._display_side(active_ladder),
            "open_trade_id": None if self._sim_active is None else self._sim_active.id,
            "trades_total": len(self._sim_trades),
            "active_contract_symbol": None if active_contract is None else active_contract.trading_symbol,
            "active_contract_security_id": None if active_contract is None else active_contract.security_id,
            "active_option_ltp": None if active_ltp is None else float(active_ltp),
            "last_error": err,
        }

    def _cfg_for_engine(self, cfg: EngineConfig) -> EngineConfig:
        if self._run_mode != "SIM" or cfg.trading_enabled:
            return cfg
        ver = self._cfg_store.version()
        if self._sim_cfg_override is None or self._sim_cfg_ver != ver:
            self._sim_cfg_override = cfg.model_copy(update={"trading_enabled": True})
            self._sim_cfg_ver = ver
        return self._sim_cfg_override

    def latency_snapshot(self) -> dict:
        snap = self._lat.snapshot()
        snap["mode"] = self._run_mode
        snap["running"] = self._running
        snap["orders_queue_size"] = int(self._orders_q.qsize())
        snap["option_ltps_size"] = int(len(self._option_ltps))
        return snap

    async def apply_order_execution(self, *, security_id: str, avg_price: float, tag: Optional[str] = None) -> None:
        """
        Apply broker execution (avg traded price) to MTM fills and strategy entry premium.

        Intended to be called by an order-update websocket listener (external or future internal).
        """
        if not self._running:
            raise RuntimeError("Engine not running.")

        secid = str(security_id)
        price = float(avg_price)
        if price <= 0:
            raise RuntimeError("avg_price must be > 0")

        cfg = self._cfg_store.current()
        cfg_eng = self._cfg_for_engine(cfg)

        def _is_entry_tag(t: Optional[str]) -> bool:
            if not t:
                return True
            return t.startswith("open_") or t.startswith("flip_open_")

        def _is_add_tag(t: Optional[str]) -> bool:
            return bool(t) and t.startswith("add_")

        def _is_close_tag(t: Optional[str]) -> bool:
            return bool(t) and (t.startswith("close_") or t.startswith("flip_close_"))

        async with self._market_lock:
            # Update MTM fill premiums for the active contract (best-effort).
            if self._run_mode != "SIM" and self._mtm_active is not None and self._mtm_active.contract.security_id == secid:
                if _is_add_tag(tag):
                    for f in reversed(self._mtm_active.fills):
                        if f.premium is None:
                            f.premium = price
                            break
                else:
                    for f in self._mtm_active.fills:
                        if f.premium is None:
                            f.premium = price
                            break

            # Strategy: apply executed entry premium for the active ladder only.
            if self._active_contract is not None and self._active_contract.security_id == secid:
                if not _is_close_tag(tag) and (_is_entry_tag(tag) or (not _is_add_tag(tag) and self._engine.adds_done == 0)):
                    self._engine.apply_execution_entry_premium(premium=price, cfg=cfg_eng)
                return

            # Not currently active: buffer only probable entry executions (so open/flip can consume it).
            if _is_entry_tag(tag) and not _is_close_tag(tag):
                self._pending_exec_entry_premiums[secid] = price

    async def _market_loop(self) -> None:
        assert self._feed is not None

        cfg = self._cfg_store.current()
        agg_tf = int(getattr(cfg, "timeframe_seconds", 60) or 60)
        if agg_tf <= 0:
            agg_tf = 60
        agg = CandleAggregator(timeframe_seconds=agg_tf)
        agg_1m = None if self._spot_candles is not None else CandleAggregator(timeframe_seconds=60)
        spot_sid = self._spot_security_id
        if spot_sid is None:
            spot_sid = str(await self._instruments.spot_security_id(symbol=self._underlying, default=cfg.spot_security_id))
            self._spot_security_id = spot_sid

        while self._running:
            try:
                sample = self._lat.next_tick_should_sample()

                t0 = self._lat.now_ns() if sample else 0
                feed_tick = await self._feed.recv_tick()
                self._feed_error = self._feed.last_error
                t1 = self._lat.now_ns() if sample else 0
                if sample:
                    self._lat.add_ns("ws_recv", t1 - t0)
                    if self._last_recv_ns is not None:
                        self._lat.add_ns("tick_interval", t1 - self._last_recv_ns)
                    self._last_recv_ns = t1
                if feed_tick is None:
                    self._lat.inc("tick_none")
                    continue
                async with self._market_lock:
                    now = feed_tick.ts if getattr(feed_tick, "ts", None) is not None else datetime.now(tz=IST)
                    ist_date = now.date().isoformat()
                    if self._last_ist_date is None:
                        self._last_ist_date = ist_date
                    elif ist_date != self._last_ist_date:
                        # New day: reset strategy engine and local state.
                        self._last_ist_date = ist_date
                        self._engine.reset_day()
                        self._active_contract = None
                        self._option_ltps.clear()
                        agg = CandleAggregator(timeframe_seconds=agg_tf)
                        if self._spot_candles is None:
                            agg_1m = CandleAggregator(timeframe_seconds=60)
                        self._last_seen_1m_end = None

                    cfg = self._cfg_store.current()
                    cfg_eng = self._cfg_for_engine(cfg)
                    self._engine.maybe_unlock_day(cfg_eng)
                    tf_now = int(getattr(cfg_eng, "timeframe_seconds", agg_tf) or agg_tf)
                    if tf_now <= 0:
                        tf_now = 60
                    if tf_now != agg_tf:
                        agg_tf = tf_now
                        agg = CandleAggregator(timeframe_seconds=agg_tf)

                    # Route ticks: spot vs active option
                    if feed_tick.security_id == str(spot_sid):
                        t_spot0 = self._lat.now_ns() if sample else 0
                        tick = SpotTick(ts=now, ltp=feed_tick.ltp)
                        t_agg0 = self._lat.now_ns() if sample else 0
                        completed = agg.push(tick)
                        completed_1m = None
                        if agg_1m is not None:
                            completed_1m = agg_1m.push(tick)
                        t_agg1 = self._lat.now_ns() if sample else 0
                        if sample:
                            self._lat.add_ns("agg_push", t_agg1 - t_agg0)
                        if self._spot_candles is not None:
                            try:
                                c = getattr(self._spot_candles, "last_completed_1m", None)
                                last_1m = c(self._underlying) if callable(c) else None
                                if last_1m is not None and (
                                    self._last_seen_1m_end is None or last_1m.end > self._last_seen_1m_end
                                ):
                                    self._last_seen_1m_end = last_1m.end
                                    t_c10 = self._lat.now_ns() if sample else 0
                                    c_actions = self._engine.on_1m_candle(last_1m, cfg_eng)
                                    t_c11 = self._lat.now_ns() if sample else 0
                                    if sample:
                                        self._lat.add_ns("strategy_on_1m_candle", t_c11 - t_c10)
                                    if c_actions:
                                        await self._handle_actions(c_actions, spot=tick.ltp, cfg=cfg_eng, now=now)
                            except Exception:
                                pass
                        elif completed_1m:
                            t_c10 = self._lat.now_ns() if sample else 0
                            c_actions = self._engine.on_1m_candle(completed_1m, cfg_eng)
                            t_c11 = self._lat.now_ns() if sample else 0
                            if sample:
                                self._lat.add_ns("strategy_on_1m_candle", t_c11 - t_c10)
                            if c_actions:
                                await self._handle_actions(c_actions, spot=tick.ltp, cfg=cfg_eng, now=now)
                        if completed:
                            t_c0 = self._lat.now_ns() if sample else 0
                            self._engine.on_candle(completed, cfg_eng)
                            t_c1 = self._lat.now_ns() if sample else 0
                            if sample:
                                self._lat.add_ns("strategy_on_candle", t_c1 - t_c0)

                        t_s0 = self._lat.now_ns() if sample else 0
                        actions = self._engine.on_tick(tick, cfg_eng)
                        t_s1 = self._lat.now_ns() if sample else 0
                        if sample:
                            self._lat.add_ns("strategy_on_tick", t_s1 - t_s0)

                        t_h0 = self._lat.now_ns() if sample else 0
                        await self._handle_actions(actions, spot=tick.ltp, cfg=cfg_eng, now=now)
                        t_h1 = self._lat.now_ns() if sample else 0
                        if sample:
                            self._lat.add_ns("handle_actions", t_h1 - t_h0)
                            self._lat.add_ns("spot_tick_total", t_h1 - t_spot0)
                        self._lat.inc("spot_ticks")
                    else:
                        t_opt0 = self._lat.now_ns() if sample else 0
                        # Normalize to avoid leading-zero mismatches between scrip master and websocket ticks.
                        secid = str(feed_tick.security_id or "").strip()
                        if secid.isdigit():
                            try:
                                secid = str(int(secid))
                            except Exception:
                                pass
                        self._option_ltps[secid] = feed_tick.ltp
                        if self._run_mode == "SIM":
                            self._sim_on_option_tick(secid, feed_tick.ltp)
                        elif self._mtm_active is not None:
                            # Best-effort fill of pending entry premiums for live MTM tracking.
                            for tr in (self._mtm_active,):
                                if str(tr.contract.security_id) != secid:
                                    continue
                                filled = 0
                                for f in tr.fills:
                                    if f.premium is None:
                                        f.premium = float(feed_tick.ltp)
                                        filled += 1
                                        if filled >= 3:
                                            break

                        # Premium-driven ladder management: only act on ticks for the active contract.
                        active_contract = self._active_contract
                        if active_contract is not None and secid == str(active_contract.security_id):
                            last_spot = self._engine.last_tick
                            if last_spot is not None:
                                t_s0 = self._lat.now_ns() if sample else 0
                                actions = self._engine.on_option_tick(
                                    premium_ltp=float(feed_tick.ltp),
                                    spot_ltp=float(last_spot.ltp),
                                    cfg=cfg_eng,
                                )
                                t_s1 = self._lat.now_ns() if sample else 0
                                if sample:
                                    self._lat.add_ns("strategy_on_option_tick", t_s1 - t_s0)

                                t_h0 = self._lat.now_ns() if sample else 0
                                await self._handle_actions(actions, spot=float(last_spot.ltp), cfg=cfg_eng, now=now)
                                t_h1 = self._lat.now_ns() if sample else 0
                                if sample:
                                    self._lat.add_ns("handle_actions_opt", t_h1 - t_h0)
                        if sample:
                            self._lat.add_ns("option_tick_total", self._lat.now_ns() - t_opt0)
                        self._lat.inc("option_ticks")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = str(e)
                log.exception("market loop error: %s", e)
                self._lat.inc("market_loop_error")

    async def _handle_actions(self, actions, *, spot: float, cfg, now: datetime) -> None:
        def _is_last_trade_exit(reason: str) -> bool:
            if not bool(getattr(cfg, "last_trade", False)):
                return False
            r = str(reason or "")
            return r == "target" or r.startswith("stop_")

        if self._run_mode == "SIM":
            stop_after = False
            for action in actions:
                if isinstance(action, OpenLadder):
                    await self._sim_open_ladder(side=action.side, spot=action.spot, cfg=cfg, now=now)
                elif isinstance(action, AddLot):
                    await self._sim_add_lots(side=action.side, levels=action.levels, spot=action.spot, cfg=cfg, now=now)
                elif isinstance(action, CloseLadder):
                    final = _is_last_trade_exit(str(action.reason))
                    await self._sim_close_ladder(
                        side=action.side,
                        spot=action.spot,
                        lots_open=action.lots_open,
                        reason=action.reason,
                        flip_to=None if final else action.flip_to,
                        cfg=cfg,
                        now=now,
                    )
                    if final:
                        stop_after = True
                        break
            if stop_after:
                self._engine.reset_day()
                await self.stop()
            return

        stop_after = False
        for action in actions:
            if stop_after:
                break
            if isinstance(action, OpenLadder):
                await self._open_ladder(side=action.side, spot=action.spot, cfg=cfg, now=now)
            elif isinstance(action, AddLot):
                await self._add_lots(side=action.side, levels=action.levels, spot=action.spot, cfg=cfg, now=now)
            elif isinstance(action, CloseLadder):
                final = _is_last_trade_exit(str(action.reason))
                await self._close_ladder(
                    side=action.side,
                    spot=action.spot,
                    lots_open=action.lots_open,
                    reason=action.reason,
                    flip_to=None if final else action.flip_to,
                    cfg=cfg,
                    now=now,
                )
                if final:
                    stop_after = True
                    break

        if stop_after:
            self._engine.reset_day()
            await self.stop()

    def _sim_on_option_tick(self, security_id: str, ltp: float) -> None:
        # Fill any pending premiums for the most-recent trade(s) on this contract.
        if not self._sim_trades:
            return
        filled = 0
        for tr in reversed(self._sim_trades):
            if tr.contract.security_id != security_id:
                continue
            did = False
            for f in tr.fills:
                if f.premium is None:
                    f.premium = float(ltp)
                    did = True
            if tr.exit_ts is not None and tr.exit_premium is None:
                tr.exit_premium = float(ltp)
                did = True
            if did:
                filled += 1
            # Don't scan too far on every tick.
            if filled >= 3:
                break

    async def _sim_open_ladder_with_contract(
        self,
        *,
        side: str,
        spot: float,
        contract: OptionContract,
        cfg,
        now: datetime,
        unsubscribe_old: Optional[OptionContract] = None,
    ) -> None:
        await self._ensure_option_subscription(contract, unsubscribe_old=unsubscribe_old)

        self._sim_seq += 1
        strategy_side = str(side)
        trade_side = self._map_strategy_to_trade_side(strategy_side)
        tr = _SimTrade(
            id=self._sim_seq,
            side=self._display_side(strategy_side),
            strategy_side=strategy_side,
            trade_side=trade_side,
            kind=self._kind,
            contract=contract,
            fills=[],
        )
        self._sim_trades.append(tr)
        self._sim_active = tr
        self._active_contract = contract

        qty = max(1, contract.lot_size) * cfg.lots_per_add
        prem = self._option_ltps.get(contract.security_id)
        tr.fills.append(_SimFill(ts=now, spot=float(spot), qty=int(qty), premium=None if prem is None else float(prem)))

    async def _sim_open_ladder(self, *, side: str, spot: float, cfg, now: datetime) -> None:
        contract = await self._select_option_contract(side=side, spot=spot, now=now, cfg=cfg)
        await self._sim_open_ladder_with_contract(side=side, spot=spot, contract=contract, cfg=cfg, now=now)

    async def _sim_add_lots(self, *, side: str, levels: int, spot: float, cfg, now: datetime) -> None:
        tr = self._sim_active
        if tr is None:
            return
        if levels <= 0:
            return
        qty = max(1, tr.contract.lot_size) * cfg.lots_per_add * int(levels)
        prem = self._option_ltps.get(tr.contract.security_id)
        tr.fills.append(_SimFill(ts=now, spot=float(spot), qty=int(qty), premium=None if prem is None else float(prem)))

    async def _sim_close_ladder(
        self, *, side: str, spot: float, lots_open: int, reason: str, flip_to: Optional[str], cfg, now: datetime
    ) -> None:
        tr = self._sim_active
        if tr is None:
            return

        prem = self._option_ltps.get(tr.contract.security_id)
        tr.exit_ts = now
        tr.exit_spot = float(spot)
        tr.exit_premium = None if prem is None else float(prem)
        tr.exit_reason = str(reason)
        tr.flip_to = flip_to

        old_contract = tr.contract
        self._sim_active = None
        self._active_contract = None

        if flip_to and not self._engine.day_locked:
            new_contract = await self._select_option_contract(side=flip_to, spot=spot, now=now, cfg=cfg)
            await self._sim_open_ladder_with_contract(
                side=flip_to, spot=spot, contract=new_contract, cfg=cfg, now=now, unsubscribe_old=old_contract
            )

    async def _open_ladder(self, *, side: str, spot: float, cfg, now: datetime) -> None:
        contract = await self._select_option_contract(side=side, spot=spot, now=now, cfg=cfg)
        await self._ensure_option_subscription(contract)

        qty = max(1, contract.lot_size) * cfg.lots_per_add
        txn = "BUY" if self._kind == "BUY" else "SELL"
        tag = f"open_{self._display_side(str(side)).lower()}"
        await self._enqueue_orders([(txn, contract.security_id, qty, tag)], cfg=cfg)
        self._active_contract = contract

        if self._run_mode != "SIM":
            prem = self._option_ltps.get(contract.security_id)
            strategy_side = str(side)
            trade_side = self._map_strategy_to_trade_side(strategy_side)
            self._mtm_active = _SimTrade(
                id=0,
                side=self._display_side(strategy_side),
                strategy_side=strategy_side,
                trade_side=trade_side,
                kind=self._kind,
                contract=contract,
                fills=[_SimFill(ts=now, spot=float(spot), qty=int(qty), premium=None if prem is None else float(prem))],
            )
            pending = self._pending_exec_entry_premiums.pop(contract.security_id, None)
            if pending is not None:
                if self._mtm_active.fills and self._mtm_active.fills[0].premium is None:
                    self._mtm_active.fills[0].premium = float(pending)
                self._engine.apply_execution_entry_premium(premium=float(pending), cfg=cfg)

    async def _add_lots(self, *, side: str, levels: int, spot: float, cfg, now: datetime) -> None:
        if self._active_contract is None:
            return
        if levels <= 0:
            return
        qty = max(1, self._active_contract.lot_size) * cfg.lots_per_add * levels
        txn = "BUY" if self._kind == "BUY" else "SELL"
        tag = f"add_{self._display_side(str(side)).lower()}"
        await self._enqueue_orders([(txn, self._active_contract.security_id, qty, tag)], cfg=cfg)

        if self._run_mode != "SIM" and self._mtm_active is not None:
            prem = self._option_ltps.get(self._active_contract.security_id)
            self._mtm_active.fills.append(
                _SimFill(ts=now, spot=float(spot), qty=int(qty), premium=None if prem is None else float(prem))
            )

    async def _close_ladder(
        self, *, side: str, spot: float, lots_open: int, reason: str, flip_to: Optional[str], cfg, now: datetime
    ) -> None:
        old_contract = self._active_contract
        old_qty = await self._resolve_close_qty(contract=old_contract, lots_open=lots_open, cfg=cfg)
        open_txn = "BUY" if self._kind == "BUY" else "SELL"
        close_txn = "SELL" if self._kind == "BUY" else "BUY"
        last_trade_final = bool(getattr(cfg, "last_trade", False)) and (
            str(reason or "") == "target" or str(reason or "").startswith("stop_")
        )

        if flip_to and not self._engine.day_locked:
            # Flip: open opposite first, then close current ladder.
            new_contract = await self._select_option_contract(side=flip_to, spot=spot, now=now, cfg=cfg)
            await self._ensure_option_subscription(new_contract, unsubscribe_old=old_contract)
            new_qty = max(1, new_contract.lot_size) * cfg.lots_per_add

            ops: list[tuple[str, str, int, str]] = [
                (open_txn, new_contract.security_id, new_qty, f"flip_open_{self._display_side(str(flip_to)).lower()}")
            ]
            if old_contract and old_qty > 0:
                ops.append((close_txn, old_contract.security_id, old_qty, f"flip_close_{self._display_side(str(side)).lower()}"))
            await self._enqueue_orders(ops, cfg=cfg)
            self._active_contract = new_contract
            if self._run_mode != "SIM":
                prem = self._option_ltps.get(new_contract.security_id)
                strategy_side = str(flip_to)
                trade_side = self._map_strategy_to_trade_side(strategy_side)
                self._mtm_active = _SimTrade(
                    id=0,
                    side=self._display_side(strategy_side),
                    strategy_side=strategy_side,
                    trade_side=trade_side,
                    kind=self._kind,
                    contract=new_contract,
                    fills=[
                        _SimFill(
                            ts=now,
                            spot=float(spot),
                            qty=int(new_qty),
                            premium=None if prem is None else float(prem),
                        )
                    ],
                )
                pending = self._pending_exec_entry_premiums.pop(new_contract.security_id, None)
                if pending is not None:
                    if self._mtm_active.fills and self._mtm_active.fills[0].premium is None:
                        self._mtm_active.fills[0].premium = float(pending)
                    self._engine.apply_execution_entry_premium(premium=float(pending), cfg=cfg)
            return

        # Normal close (target / day lock / stop max losses)
        if old_contract and old_qty > 0:
            ops = [(close_txn, old_contract.security_id, old_qty, f"close_{self._display_side(str(side)).lower()}_{reason}")]
            if last_trade_final:
                await self._enqueue_orders_and_wait(ops, cfg=cfg)
            else:
                await self._enqueue_orders(ops, cfg=cfg)
        self._active_contract = None
        if self._run_mode != "SIM":
            self._mtm_active = None

    async def _select_option_contract(self, *, side: str, spot: float, now: datetime, cfg) -> OptionContract:
        strategy_side = str(side)
        trade_side = self._map_strategy_to_trade_side(strategy_side)
        spot_f = float(spot)
        if self._underlying == "BANKNIFTY":
            strike_step = 100
        else:
            strike_step = int(cfg.strike_step)
            if strike_step <= 0:
                raise ValueError("strike_step must be > 0")
        floor_strike = int(math.floor(spot_f / strike_step) * strike_step)
        ceil_strike = int(math.ceil(spot_f / strike_step) * strike_step)

        if self._underlying == "BANKNIFTY":
            # BANKNIFTY (both BUY and SELL): always 2-strike strict OTM.
            # - CALL: 2 strikes above spot (strict on exact strikes)
            # - PUT:  2 strikes below spot (strict on exact strikes)
            exact = math.isclose(spot_f, float(floor_strike), rel_tol=0.0, abs_tol=1e-9) or math.isclose(
                spot_f, float(ceil_strike), rel_tol=0.0, abs_tol=1e-9
            )
            if trade_side == "CALL":
                strike = ceil_strike + (2 * strike_step if exact else 1 * strike_step)
            else:
                strike = floor_strike - (2 * strike_step if exact else 1 * strike_step)
        else:
            if self._kind == "BUY":
                # BUY: prefer ITM premium (strict ITM if spot is exactly on a strike).
                if trade_side == "CALL":
                    strike = floor_strike
                    if math.isclose(spot_f, float(strike), rel_tol=0.0, abs_tol=1e-9):
                        strike = strike - strike_step
                else:
                    strike = ceil_strike
                    if math.isclose(spot_f, float(strike), rel_tol=0.0, abs_tol=1e-9):
                        strike = strike + strike_step
            else:
                # SELL: prefer strict OTM strikes based on the actual option being sold.
                if trade_side == "CALL":
                    strike = ceil_strike
                    if math.isclose(spot_f, float(strike), rel_tol=0.0, abs_tol=1e-9):
                        strike = strike + strike_step
                else:
                    strike = floor_strike
                    if math.isclose(spot_f, float(strike), rel_tol=0.0, abs_tol=1e-9):
                        strike = strike - strike_step

        opt_type = "CE" if trade_side == "CALL" else "PE"

        if self._underlying == "BANKNIFTY":
            monthly_offset = int(getattr(cfg, "monthly_expiry_offset", 0) or 0)
            return await self._instruments.get_monthly_option(
                symbol="BANKNIFTY",
                now_ist=now,
                strike=strike,
                option_type=opt_type,  # type: ignore[arg-type]
                expiry_offset=monthly_offset,
            )

        contract_kind = str(getattr(cfg, "contract_kind", "WEEKLY") or "WEEKLY").upper()
        if contract_kind == "MONTHLY":
            monthly_offset = int(getattr(cfg, "monthly_expiry_offset", 0) or 0)
            return await self._instruments.get_monthly_option(
                symbol="NIFTY",
                now_ist=now,
                strike=strike,
                option_type=opt_type,  # type: ignore[arg-type]
                expiry_offset=monthly_offset,
            )

        expiry_pref = str(getattr(cfg, "weekly_expiry", "CURRENT") or "CURRENT").upper()
        expiry_offset = 1 if expiry_pref == "NEXT" else 0
        return await self._instruments.get_weekly_option(
            now_ist=now,
            strike=strike,
            option_type=opt_type,  # type: ignore[arg-type]
            expiry_offset=expiry_offset,
        )

    async def _ensure_option_subscription(self, contract: OptionContract, unsubscribe_old: Optional[OptionContract] = None) -> None:
        if self._feed is None:
            return
        await self._feed.subscribe_option(contract.security_id)
        if unsubscribe_old is not None and unsubscribe_old.security_id != contract.security_id:
            await self._feed.unsubscribe_option(unsubscribe_old.security_id)

    async def _enqueue_orders(self, ops: list[tuple[str, str, int, str]], *, cfg) -> None:
        t0 = self._lat.now_ns()
        await self._orders_q.put(_OrderBatch(ops=ops, enqueued_ns=t0))
        if self._lat.should_sample():
            self._lat.add_ns("orders_put", self._lat.now_ns() - t0)
        self._lat.inc("orders_enqueued")

    async def _enqueue_orders_and_wait(self, ops: list[tuple[str, str, int, str]], *, cfg) -> None:
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        t0 = self._lat.now_ns()
        await self._orders_q.put(_OrderBatch(ops=ops, enqueued_ns=t0, done=fut))
        if self._lat.should_sample():
            self._lat.add_ns("orders_put", self._lat.now_ns() - t0)
        self._lat.inc("orders_enqueued")
        await fut

    async def _orders_worker(self) -> None:
        while self._running:
            try:
                batch = await self._orders_q.get()
                self._lat.inc("orders_dequeued")
                now_ns = self._lat.now_ns()
                if batch.enqueued_ns:
                    self._lat.add_ns("orders_queue_wait", now_ns - batch.enqueued_ns)
                try:
                    await self._execute_batch(batch)
                except asyncio.CancelledError:
                    if batch.done and not batch.done.done():
                        batch.done.cancel()
                    raise
                except Exception as e:
                    if batch.done and not batch.done.done():
                        batch.done.set_exception(e)
                    raise
                else:
                    if batch.done and not batch.done.done():
                        batch.done.set_result(None)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = str(e)
                log.exception("order worker error: %s", e)
                self._lat.inc("orders_worker_error")

    async def _manual_close_and_wait(
        self, *, side: str, spot: float, lots_open: int, reason: str, cfg, now: datetime
    ) -> None:
        old_contract = self._active_contract
        old_qty = await self._resolve_close_qty(contract=old_contract, lots_open=lots_open, cfg=cfg)
        close_txn = "SELL" if self._kind == "BUY" else "BUY"
        if old_contract and old_qty > 0:
            tag = f"close_{self._display_side(str(side)).lower()}_{reason}"
            await self._enqueue_orders_and_wait([(close_txn, old_contract.security_id, old_qty, tag)], cfg=cfg)
        self._active_contract = None
        if self._run_mode != "SIM":
            self._mtm_active = None

    async def _resolve_close_qty(self, *, contract: Optional[OptionContract], lots_open: int, cfg) -> int:
        if contract is None:
            return 0

        local_qty = max(1, contract.lot_size) * int(lots_open)
        if self._run_mode != "LIVE" or self._rest is None:
            return local_qty
        if not bool(getattr(cfg, "broker_qty_lookup", False)):
            return local_qty

        try:
            broker_qty = await asyncio.to_thread(self._rest.get_net_position_qty, security_id=contract.security_id)
        except Exception as e:
            log.warning("broker position lookup failed for secid=%s; using local qty=%s: %s", contract.security_id, local_qty, e)
            return local_qty
        return abs(int(broker_qty))

    async def _execute_batch(self, batch: _OrderBatch) -> None:
        if self._rest is None:
            return

        cfg = self._cfg_store.current()

        async def _place_one(txn: str, secid: str, qty: int, tag: str) -> None:
            if qty <= 0:
                return

            order_type = cfg.order_type
            price = 0.0
            if order_type == "LIMIT":
                ltp = self._option_ltps.get(str(secid))
                if ltp is None:
                    raise RuntimeError(
                        f"LIMIT order requested but option LTP not available for security_id={secid} (subscribe and wait)."
                    )
                if txn == "BUY":
                    price = float(ltp + cfg.limit_price_offset)
                else:
                    price = float(max(0.05, ltp - cfg.limit_price_offset))

            t0_ns = self._lat.now_ns()
            placed = await asyncio.to_thread(
                self._rest.place_intraday_option_order,
                security_id=secid,
                transaction_type="BUY" if txn == "BUY" else "SELL",
                quantity=qty,
                order_type=order_type,
                price=price,
                tag=tag,
            )
            dt_ns = self._lat.now_ns() - t0_ns
            dt_ms = dt_ns / 1_000_000.0
            self._lat.add_ns("order_place", dt_ns)
            if not placed.ok:
                self._last_error = f"order failed tag={tag} secid={secid} qty={qty} resp={placed.raw}"
                log.warning("order failed (%.1fms): tag=%s secid=%s qty=%s resp=%s", dt_ms, tag, secid, qty, placed.raw)
                self._lat.inc("order_fail")
                raise RuntimeError(self._last_error)
            else:
                log.debug("order ok (%.1fms): tag=%s secid=%s qty=%s", dt_ms, tag, secid, qty)
                self._lat.inc("order_ok")

        # Flip batches are open opposite + close current. Start the open first, but do not wait
        # for it to complete before submitting the close (avoids entry failures delaying exits).
        ops = [op for op in batch.ops if op[2] > 0]
        if len(ops) == 2:
            (txn1, secid1, qty1, tag1), (txn2, secid2, qty2, tag2) = ops
            tags = {tag1, tag2}
            is_flip = any(t.startswith("flip_open_") for t in tags) and any(t.startswith("flip_close_") for t in tags)
            if is_flip:
                if tag1.startswith("flip_open_"):
                    open_op, close_op = (txn1, secid1, qty1, tag1), (txn2, secid2, qty2, tag2)
                else:
                    open_op, close_op = (txn2, secid2, qty2, tag2), (txn1, secid1, qty1, tag1)

                open_task = asyncio.create_task(_place_one(*open_op), name=f"order_{open_op[3]}")
                # Yield once so the open task gets scheduled before we submit the close.
                await asyncio.sleep(0)
                try:
                    await _place_one(*close_op)
                finally:
                    await open_task
                return

        for txn, secid, qty, tag in batch.ops:
            await _place_one(txn, secid, qty, tag)
