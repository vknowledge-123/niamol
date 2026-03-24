from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.engine import router as engine_router
from app.api.routes.bank_engine import router as bank_engine_router
from app.api.routes.hybrid_engine import router as hybrid_engine_router
from app.api.routes.bank_hybrid_engine import router as bank_hybrid_engine_router
from app.api.routes.instruments import router as instruments_router
from app.api.routes.sell_engine import router as sell_engine_router
from app.api.routes.bank_sell_engine import router as bank_sell_engine_router
from app.api.routes.sell_sim import router as sell_sim_router
from app.api.routes.bank_sell_sim import router as bank_sell_sim_router
from app.api.routes.hybrid_sim import router as hybrid_sim_router
from app.api.routes.bank_hybrid_sim import router as bank_hybrid_sim_router
from app.api.routes.sim import router as sim_router
from app.api.routes.bank_sim import router as bank_sim_router

router = APIRouter()
router.include_router(engine_router, tags=["engine"])
router.include_router(sell_engine_router, tags=["sell-engine"])
router.include_router(hybrid_engine_router, prefix="/hybrid", tags=["hybrid-engine"])
router.include_router(instruments_router, tags=["instruments"])
router.include_router(sim_router, tags=["simulation"])
router.include_router(sell_sim_router, tags=["sell-simulation"])
router.include_router(hybrid_sim_router, prefix="/hybrid", tags=["hybrid-simulation"])

router.include_router(bank_engine_router, prefix="/bank", tags=["bank-engine"])
router.include_router(bank_sell_engine_router, prefix="/bank", tags=["bank-sell-engine"])
router.include_router(bank_hybrid_engine_router, prefix="/bank", tags=["bank-hybrid-engine"])
router.include_router(bank_sim_router, prefix="/bank", tags=["bank-simulation"])
router.include_router(bank_sell_sim_router, prefix="/bank", tags=["bank-sell-simulation"])
router.include_router(bank_hybrid_sim_router, prefix="/bank", tags=["bank-hybrid-simulation"])
