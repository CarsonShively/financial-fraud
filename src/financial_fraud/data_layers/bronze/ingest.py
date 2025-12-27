import duckdb

def build_bronze(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    *,
    table_name: str = "bronze.raw",
) -> str:
    """Create/replace a DuckDB bronze table from a parquet file and return the table name."""
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet(?)",
        [parquet_path],
    )
    return table_name