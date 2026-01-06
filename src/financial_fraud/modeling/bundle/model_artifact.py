from dataclasses import dataclass
from typing import Any, Literal

RunRole = Literal["candidate", "baseline"]

@dataclass(frozen=True)
class ModelArtifact:
    run_id: str
    artifact_version: int
    model_type: str
    model: Any
    role: RunRole = "candidate"
    threshold: float | None = None
