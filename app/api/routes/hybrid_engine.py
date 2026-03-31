from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.runtime.settings import EngineStatus

router = APIRouter()


@router.post("/engine/start", response_model=EngineStatus)
async def start_hybrid_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.refresh_spot_candles()
        await ctx.hybrid_engine.start()
        return await ctx.hybrid_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/stop", response_model=EngineStatus)
async def stop_hybrid_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.hybrid_engine.stop()
        return await ctx.hybrid_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/engine/status", response_model=EngineStatus)
async def hybrid_engine_status(request: Request) -> EngineStatus:
    return await request.app.state.ctx.hybrid_engine.status()


@router.post("/engine/unlock_day", response_model=EngineStatus)
async def hybrid_engine_unlock_day(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.hybrid_engine.unlock_day()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/squareoff_stop", response_model=EngineStatus)
async def hybrid_engine_squareoff_stop(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.hybrid_engine.square_off_and_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/squareoff_flip", response_model=EngineStatus)
async def hybrid_engine_squareoff_flip(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.hybrid_engine.square_off_and_flip()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/flip_opposite", response_model=EngineStatus)
async def hybrid_engine_flip_opposite(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.hybrid_engine.flip_opposite_after_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/continue_same", response_model=EngineStatus)
async def hybrid_engine_continue_same(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.hybrid_engine.continue_same_after_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/engine/latency")
async def hybrid_engine_latency(request: Request) -> dict:
    return request.app.state.ctx.hybrid_engine.latency_snapshot()


class OrderExecutionUpdate(BaseModel):
    security_id: str = Field(..., min_length=1)
    avg_price: float = Field(..., gt=0)
    tag: Optional[str] = None


@router.post("/engine/order_execution", response_model=EngineStatus)
async def hybrid_engine_order_execution(request: Request, upd: OrderExecutionUpdate) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.hybrid_engine.apply_order_execution(security_id=upd.security_id, avg_price=upd.avg_price, tag=upd.tag)
        return await ctx.hybrid_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
