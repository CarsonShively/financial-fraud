from __future__ import annotations

from datetime import datetime, timezone
import uuid

def make_run_id(prefix: str = "run") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:8]
    return f"{prefix}_{ts}_{suffix}"