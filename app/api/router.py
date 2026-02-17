from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.engine import router as engine_router
from app.api.routes.instruments import router as instruments_router
from app.api.routes.sell_engine import router as sell_engine_router
from app.api.routes.sell_sim import router as sell_sim_router
from app.api.routes.sim import router as sim_router

router = APIRouter()
router.include_router(engine_router, tags=["engine"])
router.include_router(sell_engine_router, tags=["sell-engine"])
router.include_router(instruments_router, tags=["instruments"])
router.include_router(sim_router, tags=["simulation"])
router.include_router(sell_sim_router, tags=["sell-simulation"])
