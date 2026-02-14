from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import orjson


def read_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        data = orjson.loads(path.read_bytes())
    except FileNotFoundError:
        return None
    except Exception:
        return None
    if isinstance(data, dict):
        return data
    return None


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))

