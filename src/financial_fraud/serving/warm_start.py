from financial_fraud.config import REVISION, TRANSACTION_LOG, REPO_ID
from financial_fraud.io.hf import download_dataset_hf
from financial_fraud.stream.stream import TxnStream
from financial_fraud.serving.steps.base import silver_base
from financial_fraud.serving.steps.validate import validate_base
from financial_fraud.redis.infra import make_entity_key

def warm_start(
    *,
    r,
    cfg,
    lua_shas: dict[str, str],
    start_step: int | None = None,
) -> int:
    parquet_path = download_dataset_hf(
        repo_id=REPO_ID,
        filename=TRANSACTION_LOG,
        revision=REVISION,
    )

    stream = TxnStream(
        parquet_path=str(parquet_path),
        start_step=start_step,
        batch_size=2048,
    )

    sha_adv = lua_shas["dest_advance"]
    sha_add = lua_shas["dest_add"]
    N = int(cfg.dest_bucket_N)

    applied = 0

    while True:
        tx = stream.next_one()
        if tx is None:
            break

        base = silver_base(tx)
        if not validate_base(base):
            continue

        step = int(base["step"])
        amount = float(base["amount"])
        dest_id = base["name_dest"]
        dest_key = make_entity_key(cfg.live_prefix, "dest", dest_id)

        r.evalsha(sha_adv, 1, dest_key, step, N)
        r.evalsha(sha_add, 1, dest_key, step, str(amount), N)

        applied += 1

    return applied