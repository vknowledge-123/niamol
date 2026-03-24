from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.runtime.settings import EngineStatus

router = APIRouter()


@router.post("/sell/engine/start", response_model=EngineStatus)
async def start_sell_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.bank_sell_engine.start()
        return await ctx.bank_sell_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/engine/stop", response_model=EngineStatus)
async def stop_sell_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.bank_sell_engine.stop()
        return await ctx.bank_sell_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sell/engine/status", response_model=EngineStatus)
async def sell_engine_status(request: Request) -> EngineStatus:
    return await request.app.state.ctx.bank_sell_engine.status()


@router.post("/sell/engine/unlock_day", response_model=EngineStatus)
async def sell_engine_unlock_day(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.bank_sell_engine.unlock_day()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/engine/squareoff_flip", response_model=EngineStatus)
async def sell_engine_squareoff_flip(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.bank_sell_engine.square_off_and_flip()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/engine/squareoff_stop", response_model=EngineStatus)
async def sell_engine_squareoff_stop(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.bank_sell_engine.square_off_and_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/engine/flip_opposite", response_model=EngineStatus)
async def sell_engine_flip_opposite(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.bank_sell_engine.flip_opposite_after_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/engine/continue_same", response_model=EngineStatus)
async def sell_engine_continue_same(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.bank_sell_engine.continue_same_after_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sell/engine/latency")
async def sell_engine_latency(request: Request) -> dict:
    return request.app.state.ctx.bank_sell_engine.latency_snapshot()


class OrderExecutionUpdate(BaseModel):
    security_id: str = Field(..., min_length=1)
    avg_price: float = Field(..., gt=0)
    tag: Optional[str] = None


@router.post("/sell/engine/order_execution", response_model=EngineStatus)
async def sell_engine_order_execution(request: Request, upd: OrderExecutionUpdate) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.bank_sell_engine.apply_order_execution(security_id=upd.security_id, avg_price=upd.avg_price, tag=upd.tag)
        return await ctx.bank_sell_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

