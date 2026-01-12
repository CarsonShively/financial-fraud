"""
Consistent entity process for parity (advance -> read -> add).
"""

from financial_fraud.redis.infra import make_entity_key
from financial_fraud.redis.reader import read_entity
from financial_fraud.serving.steps.dest_aggregates import dest_aggregates

def get_entity_features(*, r, cfg, dest_id: str, step: int, amount: float, lua_shas: dict[str, str]) -> dict[str, float]:
    dest_key = make_entity_key(cfg.live_prefix, "dest", dest_id)
    N = int(cfg.dest_bucket_N)

    sha_adv = lua_shas["dest_advance"]
    sha_add = lua_shas["dest_add"]

    did_advance = False
    try:
        r.evalsha(sha_adv, 1, dest_key, step, N)
        did_advance = True

        dest_state = read_entity(r, cfg=cfg, dest_id=dest_id)
        return dest_aggregates(dest_state=dest_state, N=N)

    finally:
        if did_advance:
            r.evalsha(sha_add, 1, dest_key, step, amount, N)
