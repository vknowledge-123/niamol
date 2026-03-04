from __future__ import annotations

import sys
import os
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("NIFTYALGO_DATA_DIR", str(Path(__file__).resolve().parents[1] / ".tmp_test_data"))

from app.main import create_app  # noqa: E402


def main() -> None:
    app = create_app()
    with TestClient(app) as client:
        r = client.get("/")
        assert r.status_code == 200, r.text
        assert "Nifty Options Ladder Trader" in r.text

        r = client.get("/api/config")
        assert r.status_code == 200, r.text
        cfg = r.json()
        assert "strike_step" in cfg
        assert "start_preference" in cfg
        assert "instant_start" in cfg
        assert "weekly_expiry" in cfg
        assert "max_adds" in cfg

        cfg["trading_enabled"] = False
        cfg["start_preference"] = "CALL"
        r = client.put("/api/config", json=cfg)
        assert r.status_code == 200, r.text

        r = client.patch("/api/config", json={"trading_enabled": True})
        assert r.status_code == 200, r.text
        assert r.json()["trading_enabled"] is True

        r = client.get("/api/engine/status")
        assert r.status_code == 200, r.text
        st = r.json()
        assert "engine_kind" in st
        assert "position" in st
        assert "adds_done" in st
        assert "max_adds" in st
        assert "weekly_expiry" in st

        r = client.post("/api/engine/start")
        assert r.status_code == 400, r.text  # expected without instruments/credentials

        r = client.post("/api/engine/squareoff_stop")
        assert r.status_code == 400, r.text  # expected when engine not running

        r = client.get("/api/sell/engine/status")
        assert r.status_code == 200, r.text

        r = client.post("/api/sell/engine/squareoff_stop")
        assert r.status_code == 400, r.text  # expected when engine not running

    print("SMOKE OK")


if __name__ == "__main__":
    main()
