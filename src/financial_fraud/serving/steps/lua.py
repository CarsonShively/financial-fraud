from __future__ import annotations

import redis

from financial_fraud.redis.infra import make_entity_key, RedisConfig
from financial_fraud.redis.lua_scripts import SCRIPT_DEST_UPDATE

def update_dest_aggregates(
    r: redis.Redis,
    *,
    cfg: RedisConfig,
    sha_map: dict[str, str],
    dest_id: str,
    step: int,
    amount: float,
    N: int,
) -> tuple[int, int]:
    dest_key = make_entity_key(cfg.live_prefix, "dest", dest_id)

    sha = sha_map["dest_update"]
    try:
        res = r.evalsha(sha, 1, dest_key, int(step), float(amount), int(N))
    except redis.exceptions.NoScriptError:
        sha = r.script_load(SCRIPT_DEST_UPDATE)
        sha_map["dest_update"] = sha
        res = r.evalsha(sha, 1, dest_key, int(step), float(amount), int(N))

    return int(res[0]), int(res[1])
