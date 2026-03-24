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
    - NIFTY + BANKNIFTY spot security ids (defaults: 13 / 25; can also be found in CSV)
    - Index options (OPTIDX) for NIFTY/BANKNIFTY by:
      - Weekly: nearest expiry date (offset allowed)
      - Monthly: nearest monthly expiry (max expiry per calendar month; offset allowed)
    """

    CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
    DISK_PATH = SCRIP_MASTER_PATH

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._loaded = False

        self._spot_security_ids: dict[str, str] = {}

        # (symbol, expiry_iso, strike, opt_type) -> OptionContract
        self._opt_cache: dict[tuple[str, str, int, str], OptionContract] = {}
        self._opt_rows: dict[str, list[dict[str, str]]] = {"NIFTY": [], "BANKNIFTY": []}

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
                opt_rows: dict[str, list[dict[str, str]]] = {"NIFTY": [], "BANKNIFTY": []}
                spot_security_ids: dict[str, str] = {}
                for row in reader:
                    exch = row.get("SEM_EXM_EXCH_ID")
                    seg = row.get("SEM_SEGMENT")
                    secid = row.get("SEM_SMST_SECURITY_ID")
                    symname = (row.get("SM_SYMBOL_NAME") or "").strip().upper()

                    if exch == "NSE" and seg == "I" and symname in ("NIFTY", "BANKNIFTY") and secid:
                        spot_security_ids[symname] = str(secid)

                    instr = row.get("SEM_INSTRUMENT_NAME")
                    tsym = row.get("SEM_TRADING_SYMBOL") or ""
                    if (
                        exch == "NSE"
                        and seg == "D"
                        and instr == "OPTIDX"
                        and (tsym.startswith("NIFTY-") or tsym.startswith("BANKNIFTY-"))
                    ):
                        if tsym.startswith("BANKNIFTY-"):
                            opt_rows["BANKNIFTY"].append(row)
                        else:
                            opt_rows["NIFTY"].append(row)

            self._opt_rows = opt_rows
            self._opt_cache.clear()
            self._spot_security_ids = spot_security_ids
            self._loaded = True

    async def spot_security_id(self, *, symbol: str, default: str) -> str:
        sym = str(symbol or "").strip().upper()
        async with self._lock:
            return self._spot_security_ids.get(sym) or str(default)

    async def nifty_spot_security_id(self, default: str = "13") -> str:
        return await self.spot_security_id(symbol="NIFTY", default=default)

    async def banknifty_spot_security_id(self, default: str = "25") -> str:
        return await self.spot_security_id(symbol="BANKNIFTY", default=default)

    async def get_current_weekly_option(
        self,
        *,
        now_ist: datetime,
        strike: int,
        option_type: Literal["CE", "PE"],
    ) -> OptionContract:
        return await self.get_weekly_option(now_ist=now_ist, strike=strike, option_type=option_type, expiry_offset=0)

    async def get_weekly_option(
        self,
        *,
        now_ist: datetime,
        strike: int,
        option_type: Literal["CE", "PE"],
        expiry_offset: int = 0,
    ) -> OptionContract:
        return await self._get_weekly_option_for_symbol(
            symbol="NIFTY",
            now_ist=now_ist,
            strike=strike,
            option_type=option_type,
            expiry_offset=expiry_offset,
        )

    async def get_monthly_option(
        self,
        *,
        symbol: Literal["NIFTY", "BANKNIFTY"],
        now_ist: datetime,
        strike: int,
        option_type: Literal["CE", "PE"],
        expiry_offset: int = 0,
    ) -> OptionContract:
        return await self._get_monthly_option_for_symbol(
            symbol=str(symbol).upper(),
            now_ist=now_ist,
            strike=strike,
            option_type=option_type,
            expiry_offset=expiry_offset,
        )

    @staticmethod
    def _parse_expiry(exp_s: str) -> Optional[datetime]:
        try:
            return datetime.strptime(exp_s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
        except ValueError:
            return None

    def _rows_for_symbol(self, symbol: str) -> list[dict[str, str]]:
        sym = str(symbol or "").strip().upper()
        return list(self._opt_rows.get(sym) or [])

    async def _get_weekly_option_for_symbol(
        self,
        *,
        symbol: str,
        now_ist: datetime,
        strike: int,
        option_type: Literal["CE", "PE"],
        expiry_offset: int = 0,
    ) -> OptionContract:
        if now_ist.tzinfo is None:
            raise ValueError("now_ist must be timezone-aware (IST).")

        async with self._lock:
            if not self._loaded:
                raise RuntimeError("Instrument master not loaded. Refresh instruments first.")

            expiry_offset_i = int(expiry_offset)
            if expiry_offset_i < 0:
                expiry_offset_i = 0

            expiries: list[datetime] = []
            for row in self._rows_for_symbol(symbol):
                exp = self._parse_expiry(row.get("SEM_EXPIRY_DATE") or "")
                if exp is None:
                    continue
                # Treat the expiry as valid for the entire calendar day.
                # Dhan's CSV timestamps (e.g. 14:30:00) don't always align with the
                # practical "tradeable until end-of-session" behavior.
                if exp.date() >= now_ist.date():
                    expiries.append(exp)

            if not expiries:
                raise RuntimeError(f"No expiry >= now found in scrip master for {symbol}.")

            expiries = sorted(set(expiries))
            if expiry_offset_i >= len(expiries):
                raise RuntimeError(
                    f"Expiry offset={expiry_offset_i} out of range for {symbol} (available={len(expiries)} from now)."
                )
            chosen_expiry = expiries[expiry_offset_i]

            expiry_iso = chosen_expiry.isoformat()
            sym = str(symbol).upper()
            key = (sym, expiry_iso, strike, option_type)
            cached = self._opt_cache.get(key)
            if cached:
                return cached

            chosen: Optional[OptionContract] = None
            for row in self._rows_for_symbol(sym):
                exp = self._parse_expiry(row.get("SEM_EXPIRY_DATE") or "")
                if exp is None:
                    continue
                if exp != chosen_expiry:
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
                raise RuntimeError(f"Option not found for {sym} strike={strike} {option_type} at {chosen_expiry}.")

            self._opt_cache[key] = chosen
            return chosen

    async def _get_monthly_option_for_symbol(
        self,
        *,
        symbol: str,
        now_ist: datetime,
        strike: int,
        option_type: Literal["CE", "PE"],
        expiry_offset: int = 0,
    ) -> OptionContract:
        if now_ist.tzinfo is None:
            raise ValueError("now_ist must be timezone-aware (IST).")

        async with self._lock:
            if not self._loaded:
                raise RuntimeError("Instrument master not loaded. Refresh instruments first.")

            expiry_offset_i = int(expiry_offset)
            if expiry_offset_i < 0:
                expiry_offset_i = 0

            sym = str(symbol).strip().upper()
            expiries: list[datetime] = []
            for row in self._rows_for_symbol(sym):
                exp = self._parse_expiry(row.get("SEM_EXPIRY_DATE") or "")
                if exp is None:
                    continue
                if exp.date() >= now_ist.date():
                    expiries.append(exp)

            if not expiries:
                raise RuntimeError(f"No expiry >= now found in scrip master for {sym}.")

            # Monthly expiry = max expiry per (year, month); choose earliest monthly >= now.
            by_month: dict[tuple[int, int], datetime] = {}
            for exp in expiries:
                key = (int(exp.year), int(exp.month))
                prev = by_month.get(key)
                if prev is None or exp > prev:
                    by_month[key] = exp
            monthly_expiries = sorted(set(by_month.values()))

            if expiry_offset_i >= len(monthly_expiries):
                raise RuntimeError(
                    f"Monthly expiry offset={expiry_offset_i} out of range for {sym} (available={len(monthly_expiries)} from now)."
                )
            chosen_expiry = monthly_expiries[expiry_offset_i]

            expiry_iso = chosen_expiry.isoformat()
            key = (sym, expiry_iso, strike, option_type)
            cached = self._opt_cache.get(key)
            if cached:
                return cached

            chosen: Optional[OptionContract] = None
            for row in self._rows_for_symbol(sym):
                exp = self._parse_expiry(row.get("SEM_EXPIRY_DATE") or "")
                if exp is None:
                    continue
                if exp != chosen_expiry:
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
                raise RuntimeError(f"Monthly option not found for {sym} strike={strike} {option_type} at {chosen_expiry}.")

            self._opt_cache[key] = chosen
            return chosen
