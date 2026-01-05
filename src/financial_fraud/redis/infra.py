from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RedisConfig:
    host: str
    port: int
    db: int
    live_prefix: str
    run_meta_prefix: str
    dest_bucket_N: int


def make_entity_key(prefix: str, entity_type: str, entity_id: str) -> str:
    return f"{prefix}{entity_type}:{entity_id}"

