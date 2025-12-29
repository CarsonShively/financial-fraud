from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib


def write_model_joblib(bundle_dir: Path, artifact_obj: Any) -> Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)

    model_path = bundle_dir / "model.joblib"
    tmp_path = model_path.with_suffix(model_path.suffix + ".tmp")

    joblib.dump(artifact_obj, tmp_path)
    tmp_path.replace(model_path)

    return model_path