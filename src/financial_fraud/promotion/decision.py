"""
Best candidate vs current contender.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class PromotionDecision:
    promote: bool
    reason: str
    primary_metric: str
    contender_primary: float
    champion_primary: Optional[float] = None
    diff: Optional[float] = None


def _holdout_average_precision(m: Dict[str, Any]) -> float:
    v = (m.get("holdout") or {}).get("average_precision")
    if v is None:
        raise ValueError("metrics missing holdout value for 'average_precision'")
    return float(v)


def _artifact_version(m: Optional[Dict[str, Any]]) -> Optional[int]:
    """Return artifact_version if present and parseable; else None."""
    if not m:
        return None
    v = m.get("artifact_version")
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return None


def decide_promotion(
    *,
    contender_metrics: Dict[str, Any],
    champion_metrics: Optional[Dict[str, Any]],
    epsilon: float = 1e-3,
) -> PromotionDecision:
    pm = "average_precision"
    c_val = _holdout_average_precision(contender_metrics)

    if champion_metrics is None:
        return PromotionDecision(
            promote=True,
            reason="No current champion (bootstrap)",
            primary_metric=pm,
            contender_primary=c_val,
            champion_primary=None,
            diff=None,
        )

    cont_ver = _artifact_version(contender_metrics)
    champ_ver = _artifact_version(champion_metrics)
    if cont_ver != champ_ver:
        return PromotionDecision(
            promote=True,
            reason=f"Artifact version change: champion={champ_ver} contender={cont_ver}",
            primary_metric=pm,
            contender_primary=c_val,
            champion_primary=None,
            diff=None,
        )

    ch_val = _holdout_average_precision(champion_metrics)
    diff = c_val - ch_val

    if diff > epsilon:
        return PromotionDecision(
            promote=True,
            reason=f"Contender improves holdout {pm} by {diff:.6f} (> {epsilon})",
            primary_metric=pm,
            contender_primary=c_val,
            champion_primary=ch_val,
            diff=diff,
        )

    return PromotionDecision(
        promote=False,
        reason=f"Tie/close: contender did not beat champion by > {epsilon} (diff={diff:.6f})",
        primary_metric=pm,
        contender_primary=c_val,
        champion_primary=ch_val,
        diff=diff,
    )
