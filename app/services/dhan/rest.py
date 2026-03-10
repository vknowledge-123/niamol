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

    def get_net_position_qty(self, *, security_id: str) -> int:
        raw = self._dhan.get_positions()
        if not isinstance(raw, dict) or raw.get("status") != "success":
            raise RuntimeError(f"get_positions failed: {raw}")

        data = raw.get("data")
        rows = data if isinstance(data, list) else []
        total = 0
        for row in rows:
            if not isinstance(row, dict):
                continue

            row_security_id = row.get("securityId") or row.get("security_id") or row.get("securityID")
            if str(row_security_id or "") != str(security_id):
                continue

            net_qty = row.get("netQty")
            if net_qty is None:
                buy_qty = row.get("buyQty") or row.get("buyQuantity") or 0
                sell_qty = row.get("sellQty") or row.get("sellQuantity") or 0
                net_qty = float(buy_qty) - float(sell_qty)
            total += int(float(net_qty))
        return total
