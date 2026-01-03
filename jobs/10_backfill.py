from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

import duckdb

from financial_fraud.config import (
    DUCKDB_PATH,
    REPO_ID,
    REVISION,
    TRANSACTION_LOG,
    ENTITY_DEST_COL,
)
from financial_fraud.data_layers.bronze.ingest import build_bronze
from financial_fraud.db.executor import SQLExecutor
from financial_fraud.io.hf import download_dataset_hf
from financial_fraud.logging_utils import setup_logging
from financial_fraud.redis.connect import connect_redis, redis_config
from financial_fraud.redis.writer import write_to_redis

SILVER_SQL_PKG = "financial_fraud.data_layers.silver"
GOLD_SQL_PKG = "financial_fraud.data_layers.gold"

BASE_SQL_FILE = "base.sql"
DEST_BUCKETS_SQL_FILE = "as_of_now.sql"

REDIS_WRITE_BATCH_SIZE = 5000

DEST_BUCKETS_TABLE = "gold.dest_buckets_24"

log = logging.getLogger(__name__)


def main() -> None:
    t0 = perf_counter()
    log.info("pipeline start")

    repo_root = Path(__file__).resolve().parents[1]
    duckdb_path = repo_root / DUCKDB_PATH
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("DuckDB path: %s", duckdb_path)

    redis_cfg = redis_config()
    log.info("Redis LIVE prefix: %s", redis_cfg.live_prefix)

    try:
        with duckdb.connect(str(duckdb_path)) as con:
            ex = SQLExecutor(con)

            log.info(
                "Downloading transaction log (repo=%s, file=%s, rev=%s)",
                REPO_ID, TRANSACTION_LOG, REVISION
            )
            local_path = download_dataset_hf(
                repo_id=REPO_ID,
                filename=TRANSACTION_LOG,
                revision=REVISION,
            )

            log.info("Building bronze table from parquet: %s", local_path)
            build_bronze(con, local_path)

            log.info("Running SQL stage: silver base (%s/%s)", SILVER_SQL_PKG, BASE_SQL_FILE)
            ex.execute_script(ex.load_sql(SILVER_SQL_PKG, BASE_SQL_FILE))

            log.info("Running SQL stage: gold dest buckets 24 (%s/%s)", GOLD_SQL_PKG, DEST_BUCKETS_SQL_FILE)
            ex.execute_script(ex.load_sql(GOLD_SQL_PKG, DEST_BUCKETS_SQL_FILE))

            log.info(
                "Connecting to Redis (%s:%s db=%s)",
                redis_cfg.host,
                redis_cfg.port,
                redis_cfg.db,
            )
            r = connect_redis(redis_cfg)

            r.set(redis_cfg.current_pointer_key, redis_cfg.live_prefix)

            log.info(
                "Writing DEST buckets(24) to Redis (table=%s, entity_col=%s)",
                DEST_BUCKETS_TABLE,
                ENTITY_DEST_COL,
            )
            dest_buckets_written, _ = write_to_redis(
                con,
                r,
                cfg=redis_cfg,
                table=DEST_BUCKETS_TABLE,
                entity_type="dest_buckets_24",
                entity_col=ENTITY_DEST_COL,
                batch_size=REDIS_WRITE_BATCH_SIZE,
            )

            log.info(
                "Redis write complete: dest_buckets_rows=%s, live_prefix=%s",
                dest_buckets_written,
                redis_cfg.live_prefix,
            )

    except Exception:
        log.exception("pipeline failed")
        raise

    log.info("pipeline completed in %.2fs", perf_counter() - t0)


if __name__ == "__main__":
    setup_logging("INFO")
    main()
