from __future__ import annotations

import platform
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List

from financial_fraud.io.atomic import atomic_write_json


def _safe_cfg_dict(cfg: Any) -> Dict[str, Any]:
    if cfg is None:
        return {}
    if is_dataclass(cfg):
        return asdict(cfg)
    if isinstance(cfg, dict):
        return dict(cfg)
    return {"cfg_repr": repr(cfg)}


def assemble_metadata_payload(
    *,
    run_id: str,
    artifact_version: int,
    model_type: str,
    feature_names: Optional[List[str]] = None,
    cfg: Any = None,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "run_id": run_id,
        "artifact_version": artifact_version,
        "model_type": model_type,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "cfg": _safe_cfg_dict(cfg),
        "python": platform.python_version(),
        "platform": platform.platform(),
    }

    if feature_names is not None:
        meta["features"] = {
            "count": len(feature_names),
            "names": feature_names,
        }

    try:
        import sklearn
        meta["sklearn_version"] = sklearn.__version__
    except Exception:
        pass

    try:
        import optuna
        meta["optuna_version"] = optuna.__version__
    except Exception:
        pass

    return meta


def write_metadata_json(bundle_dir: Path, payload: Dict[str, Any]) -> Path:
    path = bundle_dir / "metadata.json"
    atomic_write_json(path, payload)
    return path
