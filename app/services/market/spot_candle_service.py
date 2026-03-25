from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from zoneinfo import ZoneInfo

from app.services.candles.aggregator import CandleAggregator
from app.services.dhan.feed import DhanMarketFeed
from app.services.market.models import Candle, SpotTick


log = logging.getLogger("niftyalgo.spot_candles")
IST = ZoneInfo("Asia/Kolkata")


@dataclass(slots=True)
class _State:
    sid: str
    agg_1m: CandleAggregator
    last_1m: Optional[Candle] = None
    window_1m: deque[Candle] = field(default_factory=lambda: deque(maxlen=500))


class SpotCandleService:
    """
    Always-on spot candle builder for NIFTY + BANKNIFTY (1m IST candles).

    This runs independent of any trading engine so that candle-trailing features
    have the previous candle ready immediately when an engine starts.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._feed: Optional[DhanMarketFeed] = None
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._last_error: Optional[str] = None

        self._nifty: Optional[_State] = None
        self._bank: Optional[_State] = None

        self._client_id: Optional[str] = None
        self._access_token: Optional[str] = None

    def last_completed_1m(self, underlying: str) -> Optional[Candle]:
        u = str(underlying).upper()
        st = self._nifty if u == "NIFTY" else self._bank if u == "BANKNIFTY" else None
        return None if st is None else st.last_1m

    def status(self) -> dict:
        n = self._nifty
        b = self._bank

        def _dump_candle(c: Optional[Candle]) -> Optional[dict]:
            if c is None:
                return None
            return {
                "start": c.start.isoformat(),
                "end": c.end.isoformat(),
                "open": float(c.open),
                "high": float(c.high),
                "low": float(c.low),
                "close": float(c.close),
                "green": bool(c.green),
                "red": bool(c.red),
            }

        return {
            "running": bool(self._running),
            "last_error": self._last_error,
            "nifty_spot_security_id": None if n is None else str(n.sid),
            "bank_spot_security_id": None if b is None else str(b.sid),
            "nifty_last_1m": _dump_candle(None if n is None else n.last_1m),
            "bank_last_1m": _dump_candle(None if b is None else b.last_1m),
        }

    def window_1m(self, underlying: str, limit: int = 200) -> list[Candle]:
        u = str(underlying).upper()
        st = self._nifty if u == "NIFTY" else self._bank if u == "BANKNIFTY" else None
        if st is None:
            return []
        n = max(1, min(int(limit), 500))
        return list(st.window_1m)[-n:]

    async def start(
        self,
        *,
        client_id: str,
        access_token: str,
        nifty_spot_security_id: str,
        bank_spot_security_id: str,
    ) -> None:
        async with self._lock:
            if self._running:
                # No-op if creds+ids match.
                if (
                    self._client_id == str(client_id)
                    and self._access_token == str(access_token)
                    and self._nifty is not None
                    and self._bank is not None
                    and self._nifty.sid == str(nifty_spot_security_id)
                    and self._bank.sid == str(bank_spot_security_id)
                ):
                    return
                await self._stop_locked()

            self._client_id = str(client_id)
            self._access_token = str(access_token)
            nifty_sid = str(nifty_spot_security_id)
            bank_sid = str(bank_spot_security_id)
            self._nifty = _State(sid=nifty_sid, agg_1m=CandleAggregator(timeframe_seconds=60))
            self._bank = _State(sid=bank_sid, agg_1m=CandleAggregator(timeframe_seconds=60))

            self._feed = DhanMarketFeed(self._client_id, self._access_token, spot_security_id=[nifty_sid, bank_sid])
            try:
                await self._feed.connect()
            except Exception as e:
                # Keep running; websocket reconnect loop will recover.
                self._feed.notify_ws_error(e)
                self._last_error = f"spot candles: websocket connect failed: {e}"

            self._running = True
            self._task = asyncio.create_task(self._loop(), name="spot_candle_service")

    async def stop(self) -> None:
        async with self._lock:
            await self._stop_locked()

    async def _stop_locked(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        if self._feed:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._feed.disconnect()
            self._feed = None

    async def _loop(self) -> None:
        assert self._feed is not None
        assert self._nifty is not None and self._bank is not None

        while self._running:
            try:
                tick = await self._feed.recv_tick()
                if tick is None:
                    continue
                now = tick.ts if tick.ts is not None else datetime.now(tz=IST)
                sid = str(tick.security_id)
                ltp = float(tick.ltp)

                if sid == self._nifty.sid:
                    completed = self._nifty.agg_1m.push(SpotTick(ts=now, ltp=ltp))
                    if completed:
                        self._nifty.last_1m = completed
                        self._nifty.window_1m.append(completed)
                elif sid == self._bank.sid:
                    completed = self._bank.agg_1m.push(SpotTick(ts=now, ltp=ltp))
                    if completed:
                        self._bank.last_1m = completed
                        self._bank.window_1m.append(completed)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = str(e)
                log.warning("spot candle loop error: %s", e)
                await asyncio.sleep(0.2)
