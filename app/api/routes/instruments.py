from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.post("/instruments/refresh")
async def refresh_instruments(request: Request) -> dict:
    ctx = request.app.state.ctx
    await ctx.instruments.refresh_from_network()
    return {"ok": True}

