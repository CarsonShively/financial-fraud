REPO_ID = "carson-shively/financial-fraud"
REVISION = "main"

TRANSACTION_LOG = "data/bronze/offline.parquet"
ONLINE_DATA = "data/bronze/online.parquet"

TRAIN_DATA = "data/gold/train.parquet"

DUCKDB_PATH = "data/db/fraud.duckdb"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 1

REDIS_BASE_PREFIX = "fraud:features:"
REDIS_CURRENT_POINTER_KEY = "fraud:features:CURRENT"
REDIS_RUN_META_PREFIX = "fraud:features:RUN_META:"

REDIS_TTL_SECONDS = 0

ENTITY_ORIG_COL = "name_orig"
ENTITY_DEST_COL = "name_dest"

LABEL_COL    = "is_fraud"

FS_ENTITY_KEYS = [ENTITY_ORIG_COL, ENTITY_DEST_COL]

CURRENT_ARTIFACT_VERSION = 1

TRAIN_HF_PATH = "data/gold/train.parquet"