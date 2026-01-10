import duckdb
from financial_fraud.io.hf import download_dataset_hf
from financial_fraud.config import REPO_ID, TRANSACTION_LOG, REVISION

def compute_start_step(parquet_path: str, k: int) -> int:
    con = duckdb.connect()
    max_step = con.execute(
        "SELECT MAX(step) FROM read_parquet(?)",
        [parquet_path],
    ).fetchone()[0]
    con.close()
    return max(0, int(max_step) - int(k))
