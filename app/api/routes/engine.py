from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request

from pydantic import BaseModel, Field

from app.runtime.settings import EngineConfig, EngineStatus

router = APIRouter()


@router.get("/config", response_model=EngineConfig)
async def get_config(request: Request) -> EngineConfig:
    return await request.app.state.ctx.config_store.get()


@router.put("/config", response_model=EngineConfig)
async def set_config(request: Request, cfg: EngineConfig) -> EngineConfig:
    ctx = request.app.state.ctx
    saved = await ctx.config_store.set(cfg)
    await ctx.refresh_spot_candles()
    await ctx.engine.on_config_updated(saved)
    await ctx.sell_engine.on_config_updated(saved)
    await ctx.hybrid_engine.on_config_updated(saved)
    return saved


@router.patch("/config", response_model=EngineConfig)
async def patch_config(request: Request, patch: dict[str, Any]) -> EngineConfig:
    store = request.app.state.ctx.config_store
    base = await store.get()
    merged = base.model_dump()
    merged.update(patch)
    try:
        new_cfg = EngineConfig.model_validate(merged)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid config patch: {e}") from e
    ctx = request.app.state.ctx
    saved = await ctx.config_store.set(new_cfg)
    await ctx.refresh_spot_candles()
    await ctx.engine.on_config_updated(saved)
    await ctx.sell_engine.on_config_updated(saved)
    await ctx.hybrid_engine.on_config_updated(saved)
    return saved


@router.post("/engine/start", response_model=EngineStatus)
async def start_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.refresh_spot_candles()
        await ctx.engine.start()
        return await ctx.engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/stop", response_model=EngineStatus)
async def stop_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.engine.stop()
        return await ctx.engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/engine/status", response_model=EngineStatus)
async def engine_status(request: Request) -> EngineStatus:
    return await request.app.state.ctx.engine.status()


@router.post("/engine/unlock_day", response_model=EngineStatus)
async def engine_unlock_day(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.engine.unlock_day()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

@router.post("/engine/squareoff_flip", response_model=EngineStatus)
async def engine_squareoff_flip(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.engine.square_off_and_flip()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

@router.post("/engine/squareoff_stop", response_model=EngineStatus)
async def engine_squareoff_stop(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.engine.square_off_and_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/flip_opposite", response_model=EngineStatus)
async def engine_flip_opposite(request: Request) -> EngineStatus:
    """
    After a stop/TSL hit, manually flip the ladder to the opposite side.
    """
    ctx = request.app.state.ctx
    try:
        return await ctx.engine.flip_opposite_after_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/engine/continue_same", response_model=EngineStatus)
async def engine_continue_same(request: Request) -> EngineStatus:
    """
    After a stop/TSL hit, manually re-enter the same ladder side.
    """
    ctx = request.app.state.ctx
    try:
        return await ctx.engine.continue_same_after_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/engine/latency")
async def engine_latency(request: Request) -> dict:
    return request.app.state.ctx.engine.latency_snapshot()


class OrderExecutionUpdate(BaseModel):
    security_id: str = Field(..., min_length=1)
    avg_price: float = Field(..., gt=0)
    tag: Optional[str] = None


@router.post("/engine/order_execution", response_model=EngineStatus)
async def engine_order_execution(request: Request, upd: OrderExecutionUpdate) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.engine.apply_order_execution(security_id=upd.security_id, avg_price=upd.avg_price, tag=upd.tag)
        return await ctx.engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
