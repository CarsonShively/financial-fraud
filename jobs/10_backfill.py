from __future__ import annotations

import logging
from datetime import datetime, timezone
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
from financial_fraud.redis.publisher import delete_version_prefix, publish_keep_one

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
    base_prefix = redis_cfg.base_prefix

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_prefix = f"{base_prefix}v{run_ts}:"
    log.info("Redis run_prefix (staging): %s", run_prefix)

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
                live_prefix=run_prefix,
                batch_size=REDIS_WRITE_BATCH_SIZE,
            )

            log.info("Writing DEST snapshot to Redis (table=%s, entity_col=%s)", DEST_TABLE, ENTITY_DEST_COL)
            dest_written, _ = write_to_redis(
                con, r,
                cfg=redis_cfg,
                table=DEST_TABLE,
                entity_type="dest",
                entity_col=ENTITY_DEST_COL,
                live_prefix=run_prefix,
                batch_size=REDIS_WRITE_BATCH_SIZE,
            )

        except Exception:
            log.exception("Redis backfill failed; not publishing CURRENT")
            try:
                deleted = delete_version_prefix(r, prefix=run_prefix)
                log.info("Deleted failed staging prefix: %s (deleted_entities=%s)", run_prefix, deleted)
            except Exception:
                log.exception("Failed to delete staging prefix: %s", run_prefix)
            raise

        else:
            old_current = publish_keep_one(r, cfg=redis_cfg, new_prefix=run_prefix)
            log.info("Published CURRENT: %s -> %s", redis_cfg.current_pointer_key, run_prefix)

            if old_current:
                try:
                    deleted = delete_version_prefix(r, prefix=old_current)
                    log.info("Deleted old CURRENT prefix: %s (deleted_entities=%s)", old_current, deleted)
                except Exception:
                    log.exception("Failed to delete old CURRENT prefix: %s", old_current)

        log.info(
            "Redis write complete: orig_entities=%s, dest_entities=%s, current_prefix=%s",
            orig_written,
            dest_written,
            run_prefix,
        )

    log.info("pipeline completed in %.2fs", perf_counter() - t0)


if __name__ == "__main__":
    setup_logging("INFO")
    try:
        main()
    except Exception:
        log.exception("pipeline failed")
        raise
