"""
Read entities from redis.
"""

from __future__ import annotations

import redis

from financial_fraud.redis.infra import RedisConfig, make_entity_key


def read_entity(
    r: redis.Redis,
    *,
    cfg: RedisConfig,
    dest_id: str,
) -> dict[str, str]:
    dest_key = make_entity_key(cfg.live_prefix, "dest", dest_id)
    return r.hgetall(dest_key) or {}
