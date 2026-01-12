"""
Compute start transaction from most recent transaction for warm start history.
"""

import duckdb

def compute_start_step(parquet_path: str, k: int) -> int:
    con = duckdb.connect()
    max_step = con.execute(
        "SELECT MAX(step) FROM read_parquet(?)",
        [parquet_path],
    ).fetchone()[0]
    con.close()
    return max(0, int(max_step) - int(k))
