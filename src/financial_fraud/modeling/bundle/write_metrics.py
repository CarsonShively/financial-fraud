from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from financial_fraud.io.atomic import atomic_write_json


def assemble_metrics_payload(
    *,
    run_id: str,
    artifact_version: int,
    model_type: str,
    primary_metric: str,
    direction: str,
    threshold: float,
    holdout_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    primary_value = holdout_metrics.get(primary_metric)
    payload: Dict[str, Any] = {
        "run_id": run_id,
        "artifact_version": artifact_version,
        "model_type": model_type,
        "primary_metric": primary_metric,
        "direction": direction,
        "threshold": float(threshold),
        "primary_value": None if primary_value is None else float(primary_value),
        "holdout": holdout_metrics,
    }
    return payload


def write_metrics_json(bundle_dir: Path, payload: Dict[str, Any]) -> Path:
    path = bundle_dir / "metrics.json"
    atomic_write_json(path, payload)
    return path