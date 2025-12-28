from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True)
class RedisConfig:
    host: str
    port: int
    db: int
    base_prefix: str
    current_pointer_key: str
    run_meta_prefix: str

def make_run_prefix(base_prefix: str, run_id: str) -> str:
    """Return a versioned Redis key prefix for a feature store run."""
    return f"{base_prefix}v{run_id}:"

def make_entity_key(run_prefix: str, entity_type: str, entity_id: str) -> str:
    return f"{run_prefix}{entity_type}:{entity_id}"
