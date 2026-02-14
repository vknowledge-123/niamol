from __future__ import annotations

import os
from pathlib import Path


def _base_data_dir() -> Path:
    override = os.getenv("NIFTYALGO_DATA_DIR")
    if override:
        return Path(override)

    # Keep runtime files OUTSIDE the project directory so `uvicorn --reload`
    # doesn't restart the app when instruments/config are saved.
    base = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA")
    if base:
        return Path(base)
    return Path.home()


APP_DATA_DIR = _base_data_dir() / "niftyalgo"
CONFIG_PATH = APP_DATA_DIR / "config.json"
SCRIP_MASTER_PATH = APP_DATA_DIR / "dhan_scrip_master.csv"
