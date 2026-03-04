from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import ORJSONResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.web.router import router as web_router
from app.api.router import router as api_router
from app.runtime.context import AppContext


def create_app() -> FastAPI:
    app = FastAPI(title="Nifty Options Ladder Trader", version="0.1.0", default_response_class=ORJSONResponse)
    logger = logging.getLogger("uvicorn.error")

    base_dir = Path(__file__).resolve().parent
    static_dir = base_dir / "web" / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    app.include_router(web_router)
    app.include_router(api_router, prefix="/api")

    @app.exception_handler(RequestValidationError)
    async def _validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        logger.warning("Request validation failed: %s %s -> %s", request.method, request.url.path, exc.errors())
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.ctx = AppContext()
        await app.state.ctx.startup()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        ctx: AppContext = app.state.ctx
        await ctx.shutdown()

    return app


app = create_app()
