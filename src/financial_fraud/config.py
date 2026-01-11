REPO_ID = "carson-shively/financial-fraud"
REVISION = "main"

TRANSACTION_LOG = "data/bronze/offline.parquet"
ONLINE_TRANSACTIONS = "data/bronze/online.parquet"

TRAIN_DATA = "data/gold/train.parquet"

DUCKDB_PATH = "data/db/fraud.duckdb"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6380
REDIS_DB = 1

PARITY_DB = 2

REDIS_BASE_PREFIX = "fraud:features:"
REDIS_LIVE_PREFIX = f"{REDIS_BASE_PREFIX}LIVE:"
REDIS_RUN_META_PREFIX = f"{REDIS_BASE_PREFIX}RUN_META:"

DEST_BUCKET_N = 24

LABEL_COL = "is_fraud"

CURRENT_ARTIFACT_VERSION = 6