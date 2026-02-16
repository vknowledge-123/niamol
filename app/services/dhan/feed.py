from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional


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


@dataclass(slots=True)
class FeedTick:
    exchange_segment: int
    security_id: str
    ltp: float


class DhanMarketFeed:
    def __init__(self, client_id: str, access_token: str, spot_security_id: str) -> None:
        self._client_id = client_id
        self._access_token = access_token
        self._spot_security_id = str(spot_security_id)

        # Default subscription: NIFTY spot
        self._feed = DhanFeed(
            client_id=self._client_id,
            access_token=self._access_token,
            instruments=[(IDX, self._spot_security_id)],
            # Dhan v1 feed often rejects handshake (HTTP 400) on newer infra.
            # v2 uses token+clientId in the URL query and is more reliable.
            version="v2",
        )
        self._connected = False
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._lock:
            if self._connected:
                return
            await self._feed.connect()
            self._connected = True

    async def disconnect(self) -> None:
        async with self._lock:
            if not self._connected:
                return
            await self._feed.disconnect()
            self._connected = False

    async def subscribe_option(self, security_id: str) -> None:
        # Add NSE_FNO option instrument uses exchange segment NSE_FNO=2 in marketfeed constants.
        # DhanFeed marketfeed constants define NSE_FNO = 2 (byte).
        from dhanhq.marketfeed import NSE_FNO

        sym = (NSE_FNO, str(security_id))
        async with self._lock:
            self._feed.subscribe_symbols([sym])

    async def unsubscribe_option(self, security_id: str) -> None:
        from dhanhq.marketfeed import NSE_FNO

        sym = (NSE_FNO, str(security_id))
        async with self._lock:
            self._feed.unsubscribe_symbols([sym])

    async def recv_tick(self) -> Optional[FeedTick]:
        data = await self._feed.get_instrument_data()
        if not isinstance(data, dict):
            return None
        if "LTP" not in data or "security_id" not in data or "exchange_segment" not in data:
            return None
        try:
            ltp = float(data["LTP"])
        except (TypeError, ValueError):
            return None
        return FeedTick(
            exchange_segment=int(data["exchange_segment"]),
            security_id=str(data["security_id"]),
            ltp=ltp,
        )
