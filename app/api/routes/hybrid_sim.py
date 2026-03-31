from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


@router.post("/sim/start")
async def start_hybrid_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.refresh_spot_candles()
        await ctx.hybrid_engine.start(mode="SIM")
        return ctx.hybrid_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sim/stop")
async def stop_hybrid_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.hybrid_engine.stop()
        return ctx.hybrid_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sim/status")
async def hybrid_sim_status(request: Request) -> dict:
    return request.app.state.ctx.hybrid_engine.sim_status()


@router.get("/sim/trades")
async def hybrid_sim_trades(request: Request, limit: int = Query(default=200, ge=1, le=2000)) -> list[dict]:
    return request.app.state.ctx.hybrid_engine.sim_trades(limit=limit)
