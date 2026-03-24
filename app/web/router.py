from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter()


@router.get("/", include_in_schema=False)
async def index() -> FileResponse:
    base_dir = Path(__file__).resolve().parent
    # Avoid stale dashboard HTML due to browser caching (the file changes often during development).
    return FileResponse(str(base_dir / "static" / "index.html"), headers={"Cache-Control": "no-store"})
