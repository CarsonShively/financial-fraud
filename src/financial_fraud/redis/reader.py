from __future__ import annotations

from typing import Any
import logging
import redis

from financial_fraud.redis.infra import RedisConfig, make_entity_key

logger = logging.getLogger(__name__)

def read_entities(
    r: redis.Redis,
    *,
    cfg: RedisConfig,
    live_prefix: str,
    orig_id: str,
    dest_id: str,
) -> dict[str, dict[str, Any]]:
    orig_key = make_entity_key(live_prefix, "orig", orig_id)
    dest_key = make_entity_key(live_prefix, "dest", dest_id)

    pipe = r.pipeline(transaction=False)
    pipe.exists(orig_key)
    pipe.hgetall(orig_key)
    pipe.exists(dest_key)
    pipe.hgetall(dest_key)

    orig_exists, orig_raw, dest_exists, dest_raw = pipe.execute()

    if not orig_exists:
        logger.warning("ENTITY NOT FOUND (orig): id=%s key=%s", orig_id, orig_key)
    if not dest_exists:
        logger.warning("ENTITY NOT FOUND (dest): id=%s key=%s", dest_id, dest_key)


    def decode_map(m: dict[Any, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in m.items():
            ks = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
            vs = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
            out[ks] = vs
        return out

    return {
        "orig": decode_map(orig_raw),
        "dest": decode_map(dest_raw),
    }
