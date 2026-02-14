from __future__ import annotations

import asyncio
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import httpx
from zoneinfo import ZoneInfo

from app.runtime.paths import SCRIP_MASTER_PATH


IST = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True, slots=True)
class OptionContract:
    security_id: str
    trading_symbol: str
    expiry: datetime
    strike: int
    option_type: Literal["CE", "PE"]
    lot_size: int


class InstrumentStore:
    """
    Minimal instrument store backed by Dhan scrip-master CSV.

    We keep a tiny in-memory index for:
    - NIFTY spot security id (defaults to 13; can also be found in CSV)
    - NIFTY weekly options (nearest weekly expiry >= now)
    """

    CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
    DISK_PATH = SCRIP_MASTER_PATH

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._loaded = False

        self._nifty_spot_security_id: Optional[str] = None

        # (expiry_iso, strike, opt_type) -> OptionContract
        self._weekly_cache: dict[tuple[str, int, str], OptionContract] = {}
        self._weekly_rows: list[dict[str, str]] = []

    @property
    def loaded(self) -> bool:
        return self._loaded

    async def load_from_disk_if_present(self) -> None:
        if self.DISK_PATH.exists():
            await self._load_csv(self.DISK_PATH)

    async def refresh_from_network(self) -> None:
        self.DISK_PATH.parent.mkdir(parents=True, exist_ok=True)
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(self.CSV_URL)
            r.raise_for_status()
            self.DISK_PATH.write_bytes(r.content)
        await self._load_csv(self.DISK_PATH)

    async def _load_csv(self, path: Path) -> None:
        async with self._lock:
            with path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                weekly_rows: list[dict[str, str]] = []
                nifty_spot_security_id: Optional[str] = None
                for row in reader:
                    exch = row.get("SEM_EXM_EXCH_ID")
                    seg = row.get("SEM_SEGMENT")
                    secid = row.get("SEM_SMST_SECURITY_ID")
                    symname = (row.get("SM_SYMBOL_NAME") or "").strip().upper()

                    if exch == "NSE" and seg == "I" and symname == "NIFTY":
                        nifty_spot_security_id = secid

                    instr = row.get("SEM_INSTRUMENT_NAME")
                    tsym = row.get("SEM_TRADING_SYMBOL") or ""
                    exp_flag = row.get("SEM_EXPIRY_FLAG")
                    if (
                        exch == "NSE"
                        and seg == "D"
                        and instr == "OPTIDX"
                        and exp_flag == "W"
                        and tsym.startswith("NIFTY-")
                    ):
                        weekly_rows.append(row)

            self._weekly_rows = weekly_rows
            self._weekly_cache.clear()
            self._nifty_spot_security_id = nifty_spot_security_id
            self._loaded = True

    async def nifty_spot_security_id(self, default: str = "13") -> str:
        async with self._lock:
            return self._nifty_spot_security_id or default

    async def get_current_weekly_option(
        self,
        *,
        now_ist: datetime,
        strike: int,
        option_type: Literal["CE", "PE"],
    ) -> OptionContract:
        if now_ist.tzinfo is None:
            raise ValueError("now_ist must be timezone-aware (IST).")

        async with self._lock:
            if not self._loaded:
                raise RuntimeError("Instrument master not loaded. Refresh instruments first.")

            nearest_expiry: Optional[datetime] = None
            for row in self._weekly_rows:
                exp_s = row.get("SEM_EXPIRY_DATE") or ""
                try:
                    exp = datetime.strptime(exp_s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                except ValueError:
                    continue
                if exp >= now_ist and (nearest_expiry is None or exp < nearest_expiry):
                    nearest_expiry = exp

            if nearest_expiry is None:
                raise RuntimeError("No weekly expiry >= now found in scrip master.")

            expiry_iso = nearest_expiry.isoformat()
            key = (expiry_iso, strike, option_type)
            cached = self._weekly_cache.get(key)
            if cached:
                return cached

            chosen: Optional[OptionContract] = None
            for row in self._weekly_rows:
                exp_s = row.get("SEM_EXPIRY_DATE") or ""
                try:
                    exp = datetime.strptime(exp_s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                except ValueError:
                    continue
                if exp != nearest_expiry:
                    continue

                tsym = row.get("SEM_TRADING_SYMBOL") or ""
                opt = row.get("SEM_OPTION_TYPE") or ""
                if opt != option_type:
                    continue

                strike_s = row.get("SEM_STRIKE_PRICE") or ""
                try:
                    strike_row = int(float(strike_s))
                except ValueError:
                    continue
                if strike_row != strike:
                    continue

                secid = row.get("SEM_SMST_SECURITY_ID") or ""
                lot_s = row.get("SEM_LOT_UNITS") or ""
                try:
                    lot_size = int(float(lot_s))
                except ValueError:
                    lot_size = 0
                chosen = OptionContract(
                    security_id=secid,
                    trading_symbol=tsym,
                    expiry=exp,
                    strike=strike,
                    option_type=option_type,
                    lot_size=lot_size,
                )
                break

            if chosen is None:
                raise RuntimeError(f"Weekly option not found for strike={strike} {option_type} at {nearest_expiry}.")

            self._weekly_cache[key] = chosen
            return chosen
