from dataclasses import dataclass
from typing import Any

@dataclass
class ModelArtifact:
    run_id: str
    artifact_version: int
    model_type: str
    model: Any
    threshold: float = 0.95