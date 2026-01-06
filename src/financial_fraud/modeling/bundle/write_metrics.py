from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from financial_fraud.io.atomic import atomic_write_json


def assemble_metrics_payload(
    *,
    run_id: str,
    artifact_version: int,
    model_type: str,
    primary_metric: str,
    direction: str,
    threshold: Optional[float] = None,
    holdout_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    if threshold is not None and not (0.0 <= float(threshold) <= 1.0):
        raise ValueError(f"Invalid threshold {threshold!r}. Expected a float in [0, 1].")

    primary_value = holdout_metrics.get(primary_metric)

    payload: Dict[str, Any] = {
        "run_id": run_id,
        "artifact_version": artifact_version,
        "model_type": model_type,
        "primary_metric": primary_metric,
        "direction": direction,
        "primary_value": None if primary_value is None else float(primary_value),
        "holdout": holdout_metrics,
    }

    if threshold is not None:
        payload["threshold"] = float(threshold)

    return payload


def write_metrics_json(bundle_dir: Path, payload: Dict[str, Any]) -> Path:
    path = bundle_dir / "metrics.json"
    atomic_write_json(path, payload)
    return path
