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
    ENTITY_ORIG_COL,
    ENTITY_DEST_COL,
)
from financial_fraud.data_layers.bronze.ingest import build_bronze
from financial_fraud.db.executor import SQLExecutor
from financial_fraud.io.hf import download_dataset_hf
from financial_fraud.logging_utils import setup_logging
from financial_fraud.redis.connect import connect_redis, redis_config
from financial_fraud.redis.writer import write_to_redis
from financial_fraud.redis.publisher import 

SILVER_SQL_PKG = "financial_fraud.data_layers.silver"
GOLD_SQL_PKG = "financial_fraud.data_layers.gold"

BASE_SQL_FILE = "base.sql"
ENTITY_TABLES_SQL_FILE = "as_of_now.sql"

REDIS_WRITE_BATCH_SIZE = 1000

ORIG_TABLE = "gold.orig_snapshot"
DEST_TABLE = "gold.dest_snapshot"

log = logging.getLogger(__name__)


def main() -> None:
    t0 = perf_counter()
    log.info("pipeline start")

    repo_root = Path(__file__).resolve().parents[1]
    duckdb_path = repo_root / DUCKDB_PATH
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("DuckDB path: %s", duckdb_path)

    redis_cfg = redis_config()
    live_prefix = redis_cfg.base_prefix
    log.info("Redis live_prefix: %s", live_prefix)

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

        log.info("Running SQL stage: gold entity snapshots (%s/%s)", GOLD_SQL_PKG, ENTITY_TABLES_SQL_FILE)
        ex.execute_script(ex.load_sql(GOLD_SQL_PKG, ENTITY_TABLES_SQL_FILE))

        log.info("Connecting to Redis (%s:%s db=%s)", redis_cfg.host, redis_cfg.port, redis_cfg.db)
        r = connect_redis(redis_cfg)

        try: 
            log.info("Writing ORIG snapshot to Redis (table=%s, entity_col=%s)", ORIG_TABLE, ENTITY_ORIG_COL)
            orig_written, _ = write_to_redis(
                con, r,
                cfg=redis_cfg,
                table=ORIG_TABLE,
                entity_type="orig",
                entity_col=ENTITY_ORIG_COL,
                live_prefix=live_prefix,
                batch_size=REDIS_WRITE_BATCH_SIZE,
                purge_missing_entities=True,
            )

            log.info("Writing DEST snapshot to Redis (table=%s, entity_col=%s)", DEST_TABLE, ENTITY_DEST_COL)
            dest_written, _ = write_to_redis(
                con, r,
                cfg=redis_cfg,
                table=DEST_TABLE,
                entity_type="dest",
                entity_col=ENTITY_DEST_COL,
                live_prefix=live_prefix,
                batch_size=REDIS_WRITE_BATCH_SIZE,
                purge_missing_entities=True,
            )
        except Exception:
            log.exception("Redis backfill failed; not publishing current pointer")
            raise
        else:
            r.set(redis_cfg.current_pointer_key, live_prefix)
            log.info("Published current pointer: %s -> %s", redis_cfg.current_pointer_key, live_prefix)

        log.info(
            "Redis write complete: orig_rows=%s, dest_rows=%s, current_prefix=%s",
            orig_written,
            dest_written,
            live_prefix,
        )

    log.info("pipeline completed in %.2fs", perf_counter() - t0)


if __name__ == "__main__":
    setup_logging("INFO")
    try:
        main()
    except Exception:
        log.exception("pipeline failed")
        raise
