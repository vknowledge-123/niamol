from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


@router.post("/sell/sim/start")
async def start_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.bank_sell_engine.start(mode="SIM")
        return ctx.bank_sell_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/sell/sim/stop")
async def stop_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.bank_sell_engine.stop()
        return ctx.bank_sell_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sell/sim/status")
async def sim_status(request: Request) -> dict:
    return request.app.state.ctx.bank_sell_engine.sim_status()


@router.get("/sell/sim/trades")
async def sim_trades(request: Request, limit: int = Query(default=200, ge=1, le=2000)) -> list[dict]:
    return request.app.state.ctx.bank_sell_engine.sim_trades(limit=limit)

