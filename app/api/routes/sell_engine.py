from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app.runtime.settings import EngineStatus

router = APIRouter()


@router.post("/sell/engine/start", response_model=EngineStatus)
async def start_sell_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.sell_engine.start()
        return await ctx.sell_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/engine/stop", response_model=EngineStatus)
async def stop_sell_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        await ctx.sell_engine.stop()
        return await ctx.sell_engine.status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sell/engine/status", response_model=EngineStatus)
async def sell_engine_status(request: Request) -> EngineStatus:
    return await request.app.state.ctx.sell_engine.status()

@router.post("/sell/engine/squareoff_flip", response_model=EngineStatus)
async def sell_engine_squareoff_flip(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        # Backwards-compatible route: manual square-off now stops the engine (no flip).
        return await ctx.sell_engine.square_off_and_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

@router.post("/sell/engine/squareoff_stop", response_model=EngineStatus)
async def sell_engine_squareoff_stop(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
        return await ctx.sell_engine.square_off_and_stop()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sell/engine/latency")
async def sell_engine_latency(request: Request) -> dict:
    return request.app.state.ctx.sell_engine.latency_snapshot()
