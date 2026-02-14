from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from dhanhq import dhanhq as DhanHQ


OrderType = Literal["MARKET", "LIMIT"]
Txn = Literal["BUY", "SELL"]


@dataclass(frozen=True, slots=True)
class PlacedOrder:
    ok: bool
    raw: dict


class DhanRest:
    def __init__(self, client_id: str, access_token: str) -> None:
        self._dhan = DhanHQ(client_id, access_token)

    @property
    def client(self) -> DhanHQ:
        return self._dhan

    def place_intraday_option_order(
        self,
        *,
        security_id: str,
        transaction_type: Txn,
        quantity: int,
        order_type: OrderType,
        price: float,
        tag: Optional[str],
    ) -> PlacedOrder:
        dh = self._dhan

        if order_type == "MARKET":
            api_order_type = dh.MARKET
            api_price = 0
        else:
            api_order_type = dh.LIMIT
            api_price = float(price)

        raw = dh.place_order(
            security_id=str(security_id),
            exchange_segment=dh.NSE_FNO,
            transaction_type=dh.BUY if transaction_type == "BUY" else dh.SELL,
            quantity=int(quantity),
            order_type=api_order_type,
            product_type=dh.INTRA,
            price=api_price,
            validity=dh.DAY,
            tag=tag,
        )
        ok = bool(raw) and not (isinstance(raw, dict) and raw.get("status") in ("failure", "error"))
        return PlacedOrder(ok=ok, raw=raw if isinstance(raw, dict) else {"response": raw})

