from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.runtime.settings import EngineConfig, EngineStatus

router = APIRouter()


@router.get("/config", response_model=EngineConfig)
async def get_config(request: Request) -> EngineConfig:
    return await request.app.state.ctx.config_store.get()


@router.put("/config", response_model=EngineConfig)
async def set_config(request: Request, cfg: EngineConfig) -> EngineConfig:
    saved = await request.app.state.ctx.config_store.set(cfg)
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
    return await store.set(new_cfg)


@router.post("/engine/start", response_model=EngineStatus)
async def start_engine(request: Request) -> EngineStatus:
    ctx = request.app.state.ctx
    try:
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


@router.get("/engine/latency")
async def engine_latency(request: Request) -> dict:
    return request.app.state.ctx.engine.latency_snapshot()
