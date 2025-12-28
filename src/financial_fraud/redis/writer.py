from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import duckdb
import redis

from financial_fraud.redis.infra import RedisConfig, make_entity_key


def write_to_redis(
    con: duckdb.DuckDBPyConnection,
    r: redis.Redis,
    *,
    cfg: RedisConfig,
    table: str,
    entity_type: str,
    entity_col: str,
    live_prefix: str,
    batch_size: int = 1000,
) -> tuple[int, str]:
    started_at = datetime.now(timezone.utc).isoformat()

    index_key = f"{live_prefix}{entity_type}:index"
    run_meta_key = f"{cfg.run_meta_prefix}LIVE:{entity_type}"
    
    r.delete(index_key)

    r.hset(
        run_meta_key,
        mapping={
            "status": "WRITING",
            "run_prefix": live_prefix,
            "source_table": table,
            "entity_type": entity_type,
            "entity_col": entity_col,
            "started_at": started_at,
            "index_key": index_key,
            "mode": "single_state_overwrite",
        },
    )

    written_rows = 0

    try:
        cur = con.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]

        if entity_col not in cols:
            raise ValueError(f"{table} must include entity column '{entity_col}'")

        entity_idx = cols.index(entity_col)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break

            pipe = r.pipeline(transaction=False)

            for row in rows:
                entity = row[entity_idx]
                if entity is None:
                    continue

                entity_id = str(entity)
                key = make_entity_key(live_prefix, entity_type, entity_id)

                mapping: dict[str, str] = {}
                for i, col in enumerate(cols):
                    if col == entity_col:
                        continue
                    val: Any = row[i]
                    if val is None:
                        continue
                    mapping[col] = str(val)

                if not mapping:
                    continue

                pipe.delete(key)
                pipe.hset(key, mapping=mapping)

                pipe.sadd(index_key, entity_id)

                written_rows += 1

            pipe.execute()

        unique_entities = r.scard(index_key)

        r.hset(
            run_meta_key,
            mapping={
                "status": "PUBLISHED",
                "rows_written": str(written_rows),
                "unique_entities": str(unique_entities),
                "published_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        return written_rows, live_prefix

    except Exception as e:
        unique_entities = r.scard(index_key)

        r.hset(
            run_meta_key,
            mapping={
                "status": "FAILED",
                "error": repr(e),
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "rows_written": str(written_rows),
                "unique_entities": str(unique_entities),
            },
        )
        raise
