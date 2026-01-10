import duckdb
import pandas as pd

from financial_fraud.io.hf import download_dataset_hf
from financial_fraud.config import REPO_ID, TRAIN_HF_PATH, REVISION
from financial_fraud.redis.connect import connect_redis, redis_config
from financial_fraud.redis.lua.lua_scripts import SCRIPT_DEST_ADD, SCRIPT_DEST_ADVANCE
from financial_fraud.serving.steps.dest_aggregates import dest_aggregates
from financial_fraud.redis.reader import read_entity
from financial_fraud.redis.infra import make_entity_key


FEATURES = [
    "dest_txn_count_1h",
    "dest_txn_count_24h",
    "dest_amount_sum_1h",
    "dest_amount_sum_24h",
    "dest_amount_mean_24h",
    "dest_last_gap_hours",
    "dest_state_present",
    "dest_is_warm_24h",
]


def _close(a, b, tol: float = 1e-6) -> bool:
    if a is pd.NA or pd.isna(a):
        a = None
    if b is pd.NA or pd.isna(b):
        b = None
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return a == b


def main():
    local_path = download_dataset_hf(repo_id=REPO_ID, filename=TRAIN_HF_PATH, revision=REVISION)

    required = ["txn_id", "step", "name_dest", "amount", *FEATURES]

    con = duckdb.connect()
    schema_cols = {r[0] for r in con.execute("SELECT name FROM parquet_schema(?)", [str(local_path)]).fetchall()}
    missing = [c for c in required if c not in schema_cols]
    if missing:
        raise KeyError(f"train.parquet missing columns needed for parity test: {missing}")

    dest_id = con.execute(
        """
        SELECT name_dest
        FROM read_parquet(?)
        GROUP BY 1
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """,
        [str(local_path)],
    ).fetchone()[0]

    sub = con.execute(
        f"""
        SELECT {", ".join(required)}
        FROM read_parquet(?)
        WHERE name_dest = ?
        ORDER BY step, txn_id
        """,
        [str(local_path), str(dest_id)],
    ).df()

    cfg = redis_config()
    r = connect_redis(cfg)
    r.flushdb()

    sha_adv = r.script_load(SCRIPT_DEST_ADVANCE)
    sha_add = r.script_load(SCRIPT_DEST_ADD)

    N = int(cfg.dest_bucket_N)
    key = make_entity_key(cfg.live_prefix, "dest", dest_id)

    mismatches = []

    for i, row in enumerate(sub.itertuples(index=False)):
        txn_id = int(getattr(row, "txn_id"))
        step = int(getattr(row, "step"))
        amount = float(getattr(row, "amount"))

        # 1) ADVANCE first (align buckets for this step, no current txn added)
        r.evalsha(sha_adv, 1, key, step, N)

        # 2) READ + COMPUTE from pre-add state
        dest_state = read_entity(r, cfg=cfg, dest_id=dest_id)
        online = dest_aggregates(step=step, dest_state=dest_state, N=N)

        # 3) COMPARE
        for f in FEATURES:
            offline_val = getattr(row, f)
            if not _close(offline_val, online.get(f), tol=1e-6):
                mismatches.append(
                    {
                        "row": i,
                        "txn_id": txn_id,
                        "step": step,
                        "dest": dest_id,
                        "feature": f,
                        "offline": offline_val,
                        "online": online.get(f),
                    }
                )

        r.evalsha(sha_add, 1, key, step, str(amount), N)

    if mismatches:
        out = pd.DataFrame(mismatches)
        print("❌ Parity mismatches (first 50):")
        print(out.head(50).to_string(index=False))
        print(f"\nTotal mismatches: {len(mismatches)}")
    else:
        print(f"✅ Parity passed for dest={dest_id} on {len(sub)} rows")


if __name__ == "__main__":
    main()
