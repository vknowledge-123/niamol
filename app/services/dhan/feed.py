from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
import json
import logging
import random
import time
from typing import Optional

import websockets
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone


def _patch_websockets_closed_attr() -> None:
    """
    dhanhq<=2.0.2 expects `ws.closed` (legacy websockets API), but websockets>=12
    returns `ClientConnection` without a `closed` attribute.

    Add a compatible `closed` property based on the connection state.
    """

    try:
        from websockets.asyncio.client import ClientConnection  # type: ignore[import-not-found]
        from websockets.protocol import State  # type: ignore[import-not-found]
    except Exception:
        return

    if hasattr(ClientConnection, "closed"):
        return

    ClientConnection.closed = property(lambda self: self.state is State.CLOSED)  # type: ignore[attr-defined]


_patch_websockets_closed_attr()

from dhanhq.marketfeed import DhanFeed, IDX

log = logging.getLogger("niftyalgo.dhan.feed")
IST = ZoneInfo("Asia/Kolkata")


def _norm_secid(secid: object) -> str:
    s = str(secid or "").strip()
    if s.isdigit():
        try:
            return str(int(s))
        except Exception:
            return s
    return s


@dataclass(slots=True)
class FeedTick:
    exchange_segment: int
    security_id: str
    ltp: float
    ts: Optional[datetime] = None  # timezone-aware (IST) when available
    ltt: Optional[str] = None  # raw LTT as delivered by dhanhq (best-effort)


class DhanMarketFeed:
    def __init__(
        self,
        client_id: str,
        access_token: str,
        spot_security_id: str | list[str],
        *,
        enable_rest_fallback: bool = False,
    ) -> None:
        self._client_id = client_id
        self._access_token = access_token
        if isinstance(spot_security_id, list):
            spot_ids = [str(x) for x in spot_security_id if str(x)]
        else:
            spot_ids = [str(spot_security_id)]
        if not spot_ids:
            spot_ids = ["13"]
        self._spot_security_id = str(spot_ids[0])
        self._spot_security_ids = spot_ids
        self._last_ltt_utc: Optional[datetime] = None

        # Default subscription: NIFTY spot
        self._feed = DhanFeed(
            client_id=self._client_id,
            access_token=self._access_token,
            # Use 3-tuples everywhere (exchange, security_id, mode) so we can mix
            # spot ticker + option quote subscriptions without mixing tuple sizes.
            instruments=[(IDX, _norm_secid(sid), 15) for sid in self._spot_security_ids],
            # Dhan v1 feed often rejects handshake (HTTP 400) on newer infra.
            # v2 uses token+clientId in the URL query and is more reliable.
            version="v2",
        )
        self._lock = asyncio.Lock()
        self._reconnect_attempt: int = 0
        self._last_disconnect_log_ts: float = 0.0
        self.last_error: Optional[str] = None

        self._closing: bool = False
        self._reconnect_task: Optional[asyncio.Task] = None

        # If you want to tune these (e.g. behind flaky networks), make them configurable.
        self._ping_interval_s: float = 20.0
        self._ping_timeout_s: float = 20.0
        self._enable_rest_fallback: bool = bool(enable_rest_fallback)

        # REST fallback (disabled by default; websocket-only mode preferred).
        self._rest = None
        self._rest_poll_interval_s: float = 1.0
        self._rest_next_poll_ts: float = 0.0
        self._rest_buffer: list[dict] = []
        if self._enable_rest_fallback:
            from dhanhq import dhanhq as DhanHQ

            self._rest = DhanHQ(client_id, access_token)

    def _parse_ltt_to_ist(self, ltt: str) -> Optional[datetime]:
        """
        dhanhq ticker packets include 'LTT' as a UTC time-of-day string (HH:MM:SS),
        derived from an epoch value but losing the date.

        Reconstruct a best-effort UTC datetime by combining with today's UTC date
        and applying a midnight wrap heuristic, then convert to IST.
        """
        s = str(ltt or "").strip()
        if not s:
            return None
        try:
            parts = s.split(":")
            if len(parts) != 3:
                return None
            hh, mm, ss = (int(parts[0]), int(parts[1]), int(parts[2]))
            now_utc = datetime.now(timezone.utc)
            dt_utc = datetime(
                now_utc.year, now_utc.month, now_utc.day, hh, mm, ss, tzinfo=timezone.utc
            )
            last = self._last_ltt_utc
            if last is not None:
                # If time-of-day goes backwards significantly, assume midnight rollover.
                if dt_utc < last - timedelta(hours=12):
                    dt_utc = dt_utc + timedelta(days=1)
                elif dt_utc > last + timedelta(hours=12):
                    dt_utc = dt_utc - timedelta(days=1)
            self._last_ltt_utc = dt_utc
            return dt_utc.astimezone(IST)
        except Exception:
            return None

    async def connect(self) -> None:
        async with self._lock:
            if self._closing:
                return
            ws = getattr(self._feed, "ws", None)
            if ws is not None and not getattr(ws, "closed", False):
                return

            # Implement our own connect so we can control timeouts / keepalive.
            if self._feed.version == "v1":
                url = "wss://api-feed.dhan.co"
            else:
                url = (
                    f"wss://api-feed.dhan.co"
                    f"?version=2&token={self._access_token}&clientId={self._client_id}&authType=2"
                )

            ws = await websockets.connect(
                url,
                ping_interval=self._ping_interval_s,
                ping_timeout=self._ping_timeout_s,
                open_timeout=15,
                close_timeout=5,
            )
            self._feed.ws = ws  # type: ignore[attr-defined]
            if self._feed.version == "v1":
                await self._feed.authorize()
            await self._feed.subscribe_instruments()

    async def disconnect(self) -> None:
        self._closing = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        async with self._lock:
            try:
                await self._feed.disconnect()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

            # dhanhq doesn't always close the underlying websocket; do best-effort here.
            ws = getattr(self._feed, "ws", None)
            if ws is not None:
                try:
                    await ws.close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
            with contextlib.suppress(Exception):
                self._feed.ws = None  # type: ignore[attr-defined]

    async def subscribe_option(self, security_id: str) -> None:
        # Add NSE_FNO option instrument uses exchange segment NSE_FNO=2 in marketfeed constants.
        # DhanFeed marketfeed constants define NSE_FNO = 2 (byte).
        from dhanhq.marketfeed import NSE_FNO

        # Options: subscribe as QUOTE (17) for more reliable option LTP delivery on v2.
        secid = _norm_secid(security_id)
        sym = (NSE_FNO, secid, 17)
        async with self._lock:
            # Keep local instrument list updated (needed for reconnect resubscribe).
            unique_symbols_set = set(getattr(self._feed, "instruments", None) or [])
            unique_symbols_set.add(sym)
            self._feed.instruments = list(unique_symbols_set)

            ws = getattr(self._feed, "ws", None)
            if ws is None or getattr(ws, "closed", False):
                return

            # Dhan SDK's subscribe_symbols() uses ensure_future(), which can silently miss sends
            # depending on loop context; send the v2 subscription message directly.
            if str(getattr(self._feed, "version", "")).lower() == "v2":
                msg = {
                    "RequestCode": 17,  # Quote subscribe
                    "InstrumentCount": 1,
                    "InstrumentList": [
                        {
                            "ExchangeSegment": self._feed.get_exchange_segment(NSE_FNO),
                            "SecurityId": secid,
                        }
                    ],
                }
                await ws.send(json.dumps(msg))
            else:
                self._feed.subscribe_symbols([sym])

    async def unsubscribe_option(self, security_id: str) -> None:
        from dhanhq.marketfeed import NSE_FNO

        secid = _norm_secid(security_id)
        async with self._lock:
            unique_symbols_set = set(getattr(self._feed, "instruments", None) or [])
            # Remove any matching option instrument regardless of mode (15/17/21).
            to_remove = [t for t in unique_symbols_set if isinstance(t, tuple) and len(t) >= 2 and int(t[0]) == int(NSE_FNO) and _norm_secid(t[1]) == secid]  # type: ignore[index]
            for t in to_remove:
                with contextlib.suppress(KeyError):
                    unique_symbols_set.remove(t)
                self._feed.instruments = list(unique_symbols_set)

            ws = getattr(self._feed, "ws", None)
            if ws is None or getattr(ws, "closed", False):
                return

            if str(getattr(self._feed, "version", "")).lower() == "v2":
                msg = {
                    "RequestCode": 18,  # Quote unsubscribe
                    "InstrumentCount": 1,
                    "InstrumentList": [
                        {
                            "ExchangeSegment": self._feed.get_exchange_segment(NSE_FNO),
                            "SecurityId": secid,
                        }
                    ],
                }
                await ws.send(json.dumps(msg))
            else:
                # Best-effort: unsubscribe quote mode (17) by default.
                self._feed.unsubscribe_symbols([(NSE_FNO, secid, 17)])

    def _next_reconnect_delay_s(self) -> float:
        # Exponential backoff with jitter: 0.5, 1, 2, 4, ... up to 30s (+ small jitter)
        base = min(30.0, 0.5 * (2 ** max(0, self._reconnect_attempt - 1)))
        return base + random.random() * 0.25

    @staticmethod
    def _describe_disconnect(exc: BaseException) -> str:
        code = getattr(exc, "code", None)
        reason = getattr(exc, "reason", None)
        if code is not None:
            if reason:
                return f"{type(exc).__name__}: {exc} (code={code}, reason={reason})"
            return f"{type(exc).__name__}: {exc} (code={code})"
        return f"{type(exc).__name__}: {exc}"

    async def _close_ws(self) -> None:
        async with self._lock:
            ws = getattr(self._feed, "ws", None)
            if ws is not None:
                try:
                    await ws.close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
            with contextlib.suppress(Exception):
                self._feed.ws = None  # type: ignore[attr-defined]

    def _note_ws_disconnect(self, exc: BaseException) -> None:
        self._reconnect_attempt = min(self._reconnect_attempt + 1, 16)
        delay_s = self._next_reconnect_delay_s()
        # If the server rate-limits websocket connections (HTTP 429), back off more aggressively.
        try:
            status_code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
        except Exception:
            status_code = None
        if status_code == 429 or "HTTP 429" in str(exc):
            delay_s = max(delay_s, 10.0)
        desc = self._describe_disconnect(exc)
        rest_note = (
            f"Using REST LTP polling every {self._rest_poll_interval_s:.0f}s meanwhile."
            if self._enable_rest_fallback and self._rest is not None
            else "Waiting for websocket reconnect."
        )
        self.last_error = (
            f"Marketfeed disconnected ({desc}). "
            f"Retrying websocket in {delay_s:.2f}s (attempt {self._reconnect_attempt}). "
            f"{rest_note}"
        )

        now = time.monotonic()
        if now - self._last_disconnect_log_ts >= 5.0:
            self._last_disconnect_log_ts = now
            log.warning("%s", self.last_error)

    async def _reconnect_loop(self) -> None:
        try:
            while not self._closing:
                delay_s = self._next_reconnect_delay_s()
                await asyncio.sleep(delay_s)
                if self._closing:
                    return
                try:
                    await self.connect()
                    # Success: reset and clear error.
                    self._reconnect_attempt = 0
                    self.last_error = None
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._note_ws_disconnect(e)
                    await self._close_ws()
        finally:
            self._reconnect_task = None

    def _ensure_reconnect(self) -> None:
        if self._closing:
            return
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop(), name="dhan_ws_reconnect")

    def notify_ws_error(self, exc: BaseException) -> None:
        self._note_ws_disconnect(exc)
        self._ensure_reconnect()

    @staticmethod
    def _exchange_segment_str(exchange_segment: int) -> Optional[str]:
        return {
            0: "IDX_I",
            1: "NSE_EQ",
            2: "NSE_FNO",
            3: "NSE_CURRENCY",
            4: "BSE_EQ",
            5: "MCX_COMM",
            7: "BSE_CURRENCY",
            8: "BSE_FNO",
        }.get(int(exchange_segment))

    async def _poll_rest_into_buffer(self) -> None:
        if not self._enable_rest_fallback or self._rest is None:
            return
        now = time.monotonic()
        if self._rest_buffer:
            return
        if now < self._rest_next_poll_ts:
            await asyncio.sleep(max(0.0, self._rest_next_poll_ts - now))
        self._rest_next_poll_ts = time.monotonic() + self._rest_poll_interval_s

        instruments = getattr(self._feed, "instruments", None) or []
        wanted: list[tuple[int, str]] = []
        for tup in instruments:
            if not isinstance(tup, tuple) or len(tup) < 2:
                continue
            ex = tup[0]
            secid = tup[1]
            try:
                ex_i = int(ex)
            except Exception:
                continue
            wanted.append((ex_i, str(secid)))
        # Prefer emitting spot ticks early to keep the strategy moving.
        wanted.sort(key=lambda x: (0 if x[1] == self._spot_security_id else 1, x[0], x[1]))
        if not wanted:
            return

        payload: dict[str, list[int]] = {}
        for ex_i, secid in wanted:
            ex_s = self._exchange_segment_str(ex_i)
            if not ex_s:
                continue
            try:
                secid_i = int(secid)
            except Exception:
                continue
            payload.setdefault(ex_s, []).append(secid_i)

        if not payload:
            return

        async def _call_rest(method_name: str, pl: dict[str, list[int]]) -> dict:
            fn = getattr(self._rest, method_name)
            return await asyncio.wait_for(asyncio.to_thread(fn, pl), timeout=12.0)

        try:
            resp = await _call_rest("ticker_data", payload)
        except TimeoutError:
            self.last_error = "REST LTP polling failed: timeout"
            return
        except Exception as e:
            self.last_error = f"REST LTP polling failed: {self._describe_disconnect(e)}"
            return

        if not isinstance(resp, dict) or resp.get("status") != "success":
            self.last_error = f"REST LTP polling failed: {resp.get('remarks') if isinstance(resp, dict) else resp}"
            return

        data = resp.get("data")
        if isinstance(data, dict) and "data" in data:
            data = data.get("data")

        requested_exchange_keys = set(payload.keys())
        found: dict[tuple[str, str], float] = {}

        def walk(node, current_exchange: Optional[str] = None) -> None:
            if isinstance(node, dict):
                ex = current_exchange
                ex_val = node.get("exchangeSegment") or node.get("exchange_segment") or node.get("ExchangeSegment")
                if isinstance(ex_val, str) and ex_val in requested_exchange_keys:
                    ex = ex_val
                elif isinstance(ex_val, int):
                    ex_s = self._exchange_segment_str(ex_val)
                    if ex_s is not None:
                        ex = ex_s

                # If dict contains exchange buckets (e.g. {"IDX_I": [...]})
                for k, v in node.items():
                    # Pattern: {"NSE_FNO": {"49081": {"ltp": 10.5}}}
                    if ex is not None and (isinstance(k, int) or (isinstance(k, str) and k.isdigit())) and isinstance(v, dict):
                        ltp_val = (
                            v.get("LTP")
                            or v.get("ltp")
                            or v.get("last_price")
                            or v.get("lastPrice")
                            or v.get("last_traded_price")
                            or v.get("lastTradedPrice")
                        )
                        if ltp_val is not None:
                            try:
                                found[(ex, str(k))] = float(ltp_val)
                            except Exception:
                                pass

                    if isinstance(k, str) and k in requested_exchange_keys:
                        walk(v, k)
                    else:
                        walk(v, ex)

                secid_val = (
                    node.get("securityId")
                    or node.get("security_id")
                    or node.get("SecurityId")
                    or node.get("SecurityID")
                )
                ltp_val = (
                    node.get("LTP")
                    or node.get("ltp")
                    or node.get("last_price")
                    or node.get("lastPrice")
                    or node.get("last_traded_price")
                    or node.get("lastTradedPrice")
                )
                if secid_val is not None and ltp_val is not None and ex is not None:
                    try:
                        found[(ex, str(secid_val))] = float(ltp_val)
                    except Exception:
                        pass
            elif isinstance(node, list):
                for it in node:
                    walk(it, current_exchange)

        walk(data)

        # If spot isn't present, try quote endpoint (sometimes differs by segment / permissions).
        spot_ex = self._exchange_segment_str(IDX)
        if spot_ex and self._spot_security_id and (spot_ex, self._spot_security_id) not in found:
            try:
                spot_id = int(self._spot_security_id)
            except Exception:
                spot_id = None
            if spot_id is not None:
                try:
                    qresp = await _call_rest("quote_data", {spot_ex: [spot_id]})
                except Exception:
                    qresp = None
                if isinstance(qresp, dict) and qresp.get("status") == "success":
                    qdata = qresp.get("data")
                    if isinstance(qdata, dict) and "data" in qdata:
                        qdata = qdata.get("data")
                    walk(qdata)

        for ex_i, secid in wanted:
            ex_s = self._exchange_segment_str(ex_i)
            if not ex_s:
                continue
            ltp = found.get((ex_s, secid))
            if ltp is None:
                ltp = found.get((ex_s, str(int(secid)))) if secid.isdigit() else None
            if ltp is None:
                continue
            self._rest_buffer.append(
                {
                    "exchange_segment": ex_i,
                    "security_id": secid,
                    "LTP": ltp,
                }
            )

    async def recv_tick(self) -> Optional[FeedTick]:
        data = None
        ws = getattr(self._feed, "ws", None)
        if ws is not None and not getattr(ws, "closed", False):
            try:
                data = await self._feed.get_instrument_data()
            except Exception as e:
                try:
                    from websockets.exceptions import ConnectionClosed
                except Exception:
                    ConnectionClosed = ()  # type: ignore[assignment]

                is_ws_attr_error = isinstance(e, AttributeError) and ("recv" in str(e) or "ws" in str(e))
                if isinstance(e, ConnectionClosed) or isinstance(e, OSError) or is_ws_attr_error:
                    self._note_ws_disconnect(e)
                    await self._close_ws()
                    self._ensure_reconnect()
                    data = None
                else:
                    raise
        else:
            self._ensure_reconnect()

        if data is None:
            if self._enable_rest_fallback:
                await self._poll_rest_into_buffer()
                if self._rest_buffer:
                    data = self._rest_buffer.pop(0)
            if data is None:
                # Prevent a tight loop when websocket is down and REST is disabled.
                await asyncio.sleep(0.05)
                return None

        if not isinstance(data, dict):
            return None
        if "LTP" not in data or "security_id" not in data or "exchange_segment" not in data:
            return None
        try:
            ltp = float(data["LTP"])
        except (TypeError, ValueError):
            return None
        ltt = data.get("LTT")
        ts = None
        if ltt is not None:
            ts = self._parse_ltt_to_ist(str(ltt))
        return FeedTick(
            exchange_segment=int(data["exchange_segment"]),
            security_id=_norm_secid(data["security_id"]),
            ltp=ltp,
            ts=ts,
            ltt=None if ltt is None else str(ltt),
        )
