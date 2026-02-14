from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from zoneinfo import ZoneInfo

from app.runtime.instruments import InstrumentStore, OptionContract
from app.runtime.settings import EngineConfigStore, EngineStatus
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


class EngineController:
    def __init__(self, config_store: EngineConfigStore, instruments: InstrumentStore) -> None:
        self._cfg_store = config_store
        self._instruments = instruments

        self._engine = StrategyEngine()
        self._running = False
        self._lock = asyncio.Lock()

        self._market_task: Optional[asyncio.Task] = None
        self._orders_task: Optional[asyncio.Task] = None
        self._orders_q: asyncio.Queue[_OrderBatch] = asyncio.Queue(maxsize=2048)

        self._feed: Optional[DhanMarketFeed] = None
        self._rest: Optional[DhanRest] = None

        self._active_contract: Optional[OptionContract] = None
        self._option_ltps: dict[str, float] = {}
        self._last_error: Optional[str] = None
        self._last_ist_date: Optional[str] = None
        self._spot_security_id: Optional[str] = None

    async def start(self) -> None:
        async with self._lock:
            if self._running:
                return

            try:
                cfg = await self._cfg_store.get()
                if not cfg.trading_enabled:
                    msg = "Set trading_enabled=true in config before starting."
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

                spot_sid = await self._instruments.nifty_spot_security_id(default=cfg.nifty_spot_security_id)
                self._spot_security_id = str(spot_sid)

                self._feed = DhanMarketFeed(cfg.client_id, cfg.access_token, spot_security_id=self._spot_security_id)
                try:
                    await self._feed.connect()
                except Exception as e:
                    msg = f"Dhan marketfeed connection failed: {e}"
                    self._last_error = msg
                    raise RuntimeError(msg) from e
                self._rest = DhanRest(cfg.client_id, cfg.access_token)

                # Simple auth check (raises if token invalid / network issue)
                try:
                    self._rest.client.get_fund_limits()
                except Exception as e:
                    msg = f"Dhan authentication failed: {e}"
                    self._last_error = msg
                    raise RuntimeError(msg) from e

                self._engine.reset_day()
                self._active_contract = None
                self._option_ltps.clear()

                self._running = True
                self._last_error = None

                self._orders_task = asyncio.create_task(self._orders_worker(), name="orders_worker")
                self._market_task = asyncio.create_task(self._market_loop(), name="market_loop")
            except Exception as e:
                # Best-effort cleanup if partial init happened.
                self._running = False
                if self._feed:
                    with contextlib.suppress(Exception):
                        await self._feed.disconnect()
                    self._feed = None
                self._rest = None
                if self._last_error is None:
                    self._last_error = str(e)
                if isinstance(e, RuntimeError):
                    raise
                raise RuntimeError(self._last_error) from e

    async def stop(self) -> None:
        async with self._lock:
            self._running = False

        if self._market_task:
            self._market_task.cancel()
            with contextlib.suppress(Exception):
                await self._market_task
            self._market_task = None

        if self._orders_task:
            self._orders_task.cancel()
            with contextlib.suppress(Exception):
                await self._orders_task
            self._orders_task = None

        if self._feed:
            with contextlib.suppress(Exception):
                await self._feed.disconnect()
            self._feed = None

        self._active_contract = None
        self._option_ltps.clear()

    async def status(self) -> EngineStatus:
        cfg = self._cfg_store.current()
        tick = self._engine.last_tick
        lots_open = self._engine.lots_open
        return EngineStatus(
            running=self._running,
            trading_enabled=cfg.trading_enabled,
            mode=self._engine.mode.value,
            active_ladder=self._engine.active_side,
            spot_ltp=None if tick is None else tick.ltp,
            entry_spot=self._engine.entry_spot,
            stop_spot=self._engine.stop_spot,
            next_add_spot=self._engine.next_add_spot,
            lots_open=lots_open,
            loss_count=self._engine.loss_count,
            day_locked=self._engine.day_locked,
            last_error=self._last_error,
        )

    async def _market_loop(self) -> None:
        assert self._feed is not None

        cfg = self._cfg_store.current()
        agg = CandleAggregator(timeframe_seconds=cfg.timeframe_seconds)
        spot_sid = self._spot_security_id
        if spot_sid is None:
            spot_sid = str(await self._instruments.nifty_spot_security_id(default=cfg.nifty_spot_security_id))
            self._spot_security_id = spot_sid

        while self._running:
            try:
                feed_tick = await self._feed.recv_tick()
                if feed_tick is None:
                    continue

                now = datetime.now(tz=IST)
                ist_date = now.date().isoformat()
                if self._last_ist_date is None:
                    self._last_ist_date = ist_date
                elif ist_date != self._last_ist_date:
                    # New day: reset strategy engine and local state.
                    self._last_ist_date = ist_date
                    self._engine.reset_day()
                    self._active_contract = None
                    self._option_ltps.clear()

                cfg = self._cfg_store.current()

                # Route ticks: spot vs active option
                if feed_tick.security_id == str(spot_sid):
                    tick = SpotTick(ts=now, ltp=feed_tick.ltp)
                    completed = agg.push(tick)
                    if completed:
                        self._engine.on_candle(completed, cfg)
                    actions = self._engine.on_tick(tick, cfg)
                    await self._handle_actions(actions, spot=tick.ltp, cfg=cfg, now=now)
                else:
                    self._option_ltps[feed_tick.security_id] = feed_tick.ltp
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = str(e)
                log.exception("market loop error: %s", e)

    async def _handle_actions(self, actions, *, spot: float, cfg, now: datetime) -> None:
        for action in actions:
            if isinstance(action, OpenLadder):
                await self._open_ladder(side=action.side, spot=action.spot, cfg=cfg, now=now)
            elif isinstance(action, AddLot):
                await self._add_lots(side=action.side, levels=action.levels, spot=action.spot, cfg=cfg)
            elif isinstance(action, CloseLadder):
                await self._close_ladder(
                    side=action.side,
                    spot=action.spot,
                    lots_open=action.lots_open,
                    reason=action.reason,
                    flip_to=action.flip_to,
                    cfg=cfg,
                    now=now,
                )

    async def _open_ladder(self, *, side: str, spot: float, cfg, now: datetime) -> None:
        contract = await self._select_option_contract(side=side, spot=spot, now=now, cfg=cfg)
        await self._ensure_option_subscription(contract)

        qty = max(1, contract.lot_size) * cfg.lots_per_add
        await self._enqueue_orders([("BUY", contract.security_id, qty, f"open_{side.lower()}")], cfg=cfg)
        self._active_contract = contract

    async def _add_lots(self, *, side: str, levels: int, spot: float, cfg) -> None:
        if self._active_contract is None:
            return
        if levels <= 0:
            return
        qty = max(1, self._active_contract.lot_size) * cfg.lots_per_add * levels
        await self._enqueue_orders([("BUY", self._active_contract.security_id, qty, f"add_{side.lower()}")], cfg=cfg)

    async def _close_ladder(
        self, *, side: str, spot: float, lots_open: int, reason: str, flip_to: Optional[str], cfg, now: datetime
    ) -> None:
        old_contract = self._active_contract
        old_qty = (max(1, old_contract.lot_size) * int(lots_open)) if old_contract else 0

        if flip_to and not self._engine.day_locked:
            # Low-latency flip requirement: BUY opposite first, then SELL all old lots.
            new_contract = await self._select_option_contract(side=flip_to, spot=spot, now=now, cfg=cfg)
            await self._ensure_option_subscription(new_contract, unsubscribe_old=old_contract)
            new_qty = max(1, new_contract.lot_size) * cfg.lots_per_add

            ops: list[tuple[str, str, int, str]] = [("BUY", new_contract.security_id, new_qty, f"flip_open_{flip_to.lower()}")]
            if old_contract and old_qty > 0:
                ops.append(("SELL", old_contract.security_id, old_qty, f"flip_close_{side.lower()}"))
            await self._enqueue_orders(ops, cfg=cfg)
            self._active_contract = new_contract
            return

        # Normal close (target / day lock / stop max losses)
        if old_contract and old_qty > 0:
            await self._enqueue_orders([("SELL", old_contract.security_id, old_qty, f"close_{side.lower()}_{reason}")], cfg=cfg)
        self._active_contract = None

    async def _select_option_contract(self, *, side: str, spot: float, now: datetime, cfg) -> OptionContract:
        strike_step = int(cfg.strike_step)
        if strike_step <= 0:
            raise ValueError("strike_step must be > 0")

        spot_i = int(round(spot))
        if side == "CALL":
            strike = (spot_i // strike_step) * strike_step
            opt_type = "CE"
        else:
            strike = ((spot_i + strike_step - 1) // strike_step) * strike_step
            opt_type = "PE"

        return await self._instruments.get_current_weekly_option(now_ist=now, strike=strike, option_type=opt_type)  # type: ignore[arg-type]

    async def _ensure_option_subscription(self, contract: OptionContract, unsubscribe_old: Optional[OptionContract] = None) -> None:
        if self._feed is None:
            return
        await self._feed.subscribe_option(contract.security_id)
        if unsubscribe_old is not None and unsubscribe_old.security_id != contract.security_id:
            await self._feed.unsubscribe_option(unsubscribe_old.security_id)

    async def _enqueue_orders(self, ops: list[tuple[str, str, int, str]], *, cfg) -> None:
        await self._orders_q.put(_OrderBatch(ops=ops))

    async def _orders_worker(self) -> None:
        while self._running:
            try:
                batch = await self._orders_q.get()
                await self._execute_batch(batch)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = str(e)
                log.exception("order worker error: %s", e)

    async def _execute_batch(self, batch: _OrderBatch) -> None:
        if self._rest is None:
            return

        cfg = self._cfg_store.current()
        for txn, secid, qty, tag in batch.ops:
            if qty <= 0:
                continue

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

            self._rest.place_intraday_option_order(
                security_id=secid,
                transaction_type="BUY" if txn == "BUY" else "SELL",
                quantity=qty,
                order_type=order_type,
                price=price,
                tag=tag,
            )
