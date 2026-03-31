from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


@router.post("/hybrid/sim/start")
async def start_bank_hybrid_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.refresh_spot_candles()
        await ctx.bank_hybrid_engine.start(mode="SIM")
        return ctx.bank_hybrid_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/hybrid/sim/stop")
async def stop_bank_hybrid_sim(request: Request) -> dict:
    ctx = request.app.state.ctx
    try:
        await ctx.bank_hybrid_engine.stop()
        return ctx.bank_hybrid_engine.sim_status()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/hybrid/sim/status")
async def bank_hybrid_sim_status(request: Request) -> dict:
    return request.app.state.ctx.bank_hybrid_engine.sim_status()


@router.get("/hybrid/sim/trades")
async def bank_hybrid_sim_trades(request: Request, limit: int = Query(default=200, ge=1, le=2000)) -> list[dict]:
    return request.app.state.ctx.bank_hybrid_engine.sim_trades(limit=limit)
