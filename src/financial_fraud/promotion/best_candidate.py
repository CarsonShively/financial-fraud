"""
Select best candidate from runs.
"""

from __future__ import annotations

from typing import Any, Optional
import math

from financial_fraud.config import CURRENT_ARTIFACT_VERSION


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
    Fraud contender selection:

    - Only gate: metrics['artifact_version'] must equal CURRENT_ARTIFACT_VERSION
    - Rank (descending):
        1) holdout['average_precision']
        2) holdout['recall_at_top_1pct']
        3) holdout['precision_at_top_1pct']
      Tie-break: run_id (deterministic)
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

        ap = _f(hold.get("average_precision"))
        rec1 = _f(hold.get("recall_at_top_1pct"))
        prec1 = _f(hold.get("precision_at_top_1pct"))
        if ap is None or rec1 is None or prec1 is None:
            continue

        run_id = getattr(r, "run_id", "") or ""

        key = (ap, rec1, prec1, run_id)

        if best_key is None or key > best_key:
            best_key = key
            best = r

    if best is None:
        raise ValueError("No candidates passed gates")

    return best
