from __future__ import annotations

from typing import Any

from financial_fraud.redis.connect import redis_config, connect_redis
from financial_fraud.io.hf import read_model_json, load_model_hf, download_dataset_hf
from financial_fraud.config import REPO_ID, REVISION, ONLINE_TRANSACTIONS


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
    return model, champion_ptr


def connect_feature_store():
    cfg = redis_config()
    r = connect_redis(cfg)
    return r, cfg


def download_online_logs(*, repo_id: str = REPO_ID, revision: str = REVISION, filename: str = ONLINE_TRANSACTIONS) -> str:
    return download_dataset_hf(repo_id=repo_id, filename=filename, revision=revision)

