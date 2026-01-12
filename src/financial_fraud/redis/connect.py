"""
Re-usable redis connect functions.
"""

from __future__ import annotations

import redis

from financial_fraud.redis.infra import RedisConfig
from financial_fraud.config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    REDIS_LIVE_PREFIX, REDIS_RUN_META_PREFIX, DEST_BUCKET_N, PARITY_DB
)

def redis_config() -> RedisConfig:
    return RedisConfig(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        live_prefix=REDIS_LIVE_PREFIX,
        run_meta_prefix=REDIS_RUN_META_PREFIX,
        dest_bucket_N=DEST_BUCKET_N,
    )
    
def parity_redis_config() -> RedisConfig:
    return RedisConfig(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=PARITY_DB,
        live_prefix=REDIS_LIVE_PREFIX,
        run_meta_prefix=REDIS_RUN_META_PREFIX,
        dest_bucket_N=DEST_BUCKET_N,
    )

def connect_redis(cfg: RedisConfig) -> redis.Redis:
    r = redis.Redis(host=cfg.host, port=cfg.port, db=cfg.db, decode_responses=True)
    r.ping()
    return r