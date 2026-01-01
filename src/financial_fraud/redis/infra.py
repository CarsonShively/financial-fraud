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

    @property
    def live_prefix(self) -> str:
        return f"{self.base_prefix}LIVE:"
    
def make_entity_key(prefix: str, entity_type: str, entity_id: str) -> str:
    return f"{prefix}{entity_type}:{entity_id}"
