from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


@router.post("/sell/sim/start")
async def start_sell_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.sell_engine.start(mode="SIM")
        return ctx.sell_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/sim/stop")
async def stop_sell_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.sell_engine.stop()
        return ctx.sell_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sell/sim/status")
async def sell_sim_status(request: Request) -> dict:
    return request.app.state.ctx.sell_engine.sim_status()


@router.get("/sell/sim/trades")
async def sell_sim_trades(request: Request, limit: int = Query(default=200, ge=1, le=2000)) -> list[dict]:
    return request.app.state.ctx.sell_engine.sim_trades(limit=limit)

