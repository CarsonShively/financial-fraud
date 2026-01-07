from __future__ import annotations

from typing import Any

from financial_fraud.redis.connect import redis_config, connect_redis
from financial_fraud.io.hf import read_model_json, load_model_hf
from financial_fraud.redis.lua_scripts import SCRIPT_DEST_UPDATE 
from financial_fraud.config import REPO_ID, REVISION

def load_champion_model(*, repo_id: str = REPO_ID, revision: str = REVISION) -> tuple[Any, dict[str, Any]]:
    champion_ptr = read_model_json(repo_id=repo_id, revision=revision, path_in_repo="champion.json")
    if not champion_ptr:
        raise RuntimeError("No champion.json found")

    artifact = load_model_hf(
        repo_id=repo_id,
        revision=revision,
        path_in_repo=f"{champion_ptr['path_in_repo']}/model.joblib",
    )
    model = getattr(artifact, "model", artifact)
    threshold = getattr(artifact, "threshold", None)
    threshold = float(threshold) if threshold is not None else None

    return model, champion_ptr, threshold

def connect_feature_store():
    cfg = redis_config()
    r = connect_redis(cfg)
    return r, cfg

def register_lua_scripts(r) -> dict[str, str]:
    sha = r.script_load(SCRIPT_DEST_UPDATE)
    return {"dest_update": sha}
