from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel


router = APIRouter(prefix="/candles")


class CandleDTO(BaseModel):
    start: str
    end: str
    open: float
    high: float
    low: float
    close: float
    green: bool
    red: bool


class CandleServiceStatus(BaseModel):
    running: bool
    last_error: Optional[str] = None
    nifty_spot_security_id: Optional[str] = None
    bank_spot_security_id: Optional[str] = None
    nifty_last_1m: Optional[CandleDTO] = None
    bank_last_1m: Optional[CandleDTO] = None


@router.get("/status", response_model=CandleServiceStatus)
async def candles_status(request: Request) -> CandleServiceStatus:
    return CandleServiceStatus.model_validate(request.app.state.ctx.spot_candles.status())


@router.post("/start", response_model=CandleServiceStatus)
async def candles_start(request: Request) -> CandleServiceStatus:
    ctx = request.app.state.ctx
    await ctx.refresh_spot_candles()
    return CandleServiceStatus.model_validate(ctx.spot_candles.status())


@router.post("/stop", response_model=CandleServiceStatus)
async def candles_stop(request: Request) -> CandleServiceStatus:
    ctx = request.app.state.ctx
    await ctx.spot_candles.stop()
    return CandleServiceStatus.model_validate(ctx.spot_candles.status())


@router.get("/window", response_model=list[CandleDTO])
async def candles_window(
    request: Request,
    underlying: Literal["NIFTY", "BANKNIFTY"] = Query(default="NIFTY"),
    limit: int = Query(default=200, ge=1, le=500),
) -> list[CandleDTO]:
    ctx = request.app.state.ctx
    candles = ctx.spot_candles.window_1m(underlying=underlying, limit=limit)
    out: list[CandleDTO] = []
    for c in candles:
        out.append(
            CandleDTO(
                start=c.start.isoformat(),
                end=c.end.isoformat(),
                open=float(c.open),
                high=float(c.high),
                low=float(c.low),
                close=float(c.close),
                green=bool(c.green),
                red=bool(c.red),
            )
        )
    return out

