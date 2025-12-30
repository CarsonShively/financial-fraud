from __future__ import annotations

from typing import Any, Optional
import math

from financial_fraud.config import CURRENT_ARTIFACT_VERSION
from financial_fraud.modeling.config import PRIMARY_METRIC


def _f(x: Any) -> Optional[float]:
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        v = float(x)
        return v if math.isfinite(v) else None
    return None


def _i(x: Any) -> Optional[int]:
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float) and x.is_integer():
        return int(x)
    return None


def artifact_version(m: dict[str, Any]) -> Optional[int]:
    return _i(m.get("artifact_version"))


def get_best_contender(rows: list[Any]) -> Any:
    """
    Fraud v1 contender selection:

    - Only gate: metrics['artifact_version'] must equal CURRENT_ARTIFACT_VERSION
    - Best run: highest holdout precision (holdout['precision'])
    - Tie-break: run_id (deterministic)
    """
    best: Any = None
    best_key: Optional[tuple] = None

    for r in rows:
        if getattr(r, "error", None):
            continue

        m = getattr(r, "metrics", None) or {}

        ver = artifact_version(m)
        if ver != CURRENT_ARTIFACT_VERSION:
            continue

        hold = m.get("holdout", {}) or {}
        hold_precision = _f(hold.get(PRIMARY_METRIC))
        if hold_precision is None:
            continue

        run_id = getattr(r, "run_id", "") or ""

        key = (hold_precision, run_id)

        if best_key is None or key > best_key:
            best_key = key
            best = r

    if best is None:
        raise ValueError("No candidates passed gates")

    return best
