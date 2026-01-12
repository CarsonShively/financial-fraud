"""Ingest data (historical logs) -> train ready data."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from time import perf_counter

import duckdb

from financial_fraud.io.hf import download_dataset_hf, upload_dataset_hf
from financial_fraud.db.executor import SQLExecutor
from financial_fraud.data_layers.bronze.ingest import build_bronze
from financial_fraud.logging_utils import setup_logging
from financial_fraud.config import (
    REPO_ID,
    TRANSACTION_LOG,
    TRAIN_DATA,
    DUCKDB_PATH,
)

log = logging.getLogger(__name__)

SILVER_SQL_PKG = "financial_fraud.data_layers.silver"
GOLD_SQL_PKG = "financial_fraud.data_layers.gold"

BASE_SQL_FILE = "base.sql"
TRAIN_SQL_FILE = "train.sql"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--upload", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main(*, upload: bool) -> None:
    """Run the training dataset build pipeline and optionally upload the output parquet to HF."""
    t0 = perf_counter()
    log.info("build_train start (upload=%s, duckdb_path=%s)", upload, DUCKDB_PATH)

    repo_root = Path(__file__).resolve().parents[1]
    duckdb_path = repo_root / DUCKDB_PATH
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    out_path = repo_root / TRAIN_DATA
    out_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("DuckDB path: %s", duckdb_path)
    log.info("Output parquet: %s", out_path)

    with duckdb.connect(duckdb_path) as con:
        ex = SQLExecutor(con)

        log.info("Downloading bronze: repo=%s file=%s", REPO_ID, TRANSACTION_LOG)
        local_bronze = download_dataset_hf(repo_id=REPO_ID, filename=TRANSACTION_LOG)
        log.info("Bronze local path: %s", local_bronze)

        log.info("Building bronze")
        build_bronze(con, local_bronze)

        log.info("Running SQL stage: base")
        ex.execute_script(ex.load_sql(SILVER_SQL_PKG, BASE_SQL_FILE))

        log.info("Running SQL stage: train table")
        ex.execute_script(ex.load_sql(GOLD_SQL_PKG, TRAIN_SQL_FILE))

        nrows = con.execute("SELECT COUNT(*) FROM gold.train").fetchone()[0]
        log.info("gold.train rows: %s", nrows)

        if nrows == 0:
            raise RuntimeError("gold.train is empty (0 rows)")

        log.info("Writing parquet")
        ex.write_parquet("SELECT * FROM gold.train", str(out_path))

    if out_path.exists():
        log.info("Wrote parquet size_bytes=%s", out_path.stat().st_size)

    if upload:
        log.info("Uploading to HF: repo=%s dest=%s", REPO_ID, TRAIN_DATA)
        upload_dataset_hf(local_path=str(out_path), repo_id=REPO_ID, hf_path=TRAIN_DATA)
        log.info("Upload complete")

    log.info("build_train done in %.2fs", perf_counter() - t0)


if __name__ == "__main__":
    args = parse_args()
    setup_logging(args.log_level)

    try:
        main(upload=args.upload)
    except Exception:
        log.exception("build_train failed")
        raise