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
    batch_size: int,
) -> tuple[int, str]:
    started_at = datetime.now(timezone.utc).isoformat()

    live_prefix = cfg.live_prefix
    run_meta_key = f"{cfg.run_meta_prefix}LIVE:{entity_type}"

    written_rows = 0
    phase = "init"

    r.hset(
        run_meta_key,
        mapping={
            "status": "WRITING",
            "run_prefix": live_prefix,
            "source_table": table,
            "entity_type": entity_type,
            "entity_col": entity_col,
            "started_at": started_at,
        },
    )

    try:
        phase = "duckdb_execute"
        cur = con.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]

        if entity_col not in cols:
            raise ValueError(f"{table} must include entity column '{entity_col}'")

        entity_idx = cols.index(entity_col)

        phase = "write_batches"
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

                pipe.hset(key, mapping=mapping)
                written_rows += 1

            pipe.execute()

        phase = "finalize_counts"
        unique_entities = int(
            con.execute(f"SELECT COUNT(DISTINCT {entity_col}) FROM {table}").fetchone()[0]
        )

        phase = "meta_ready"
        r.hset(
            run_meta_key,
            mapping={
                "status": "READY",
                "rows_written": str(written_rows),
                "unique_entities": str(unique_entities),
                "ready_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        return written_rows, live_prefix

    except Exception as e:
        try:
            r.hset(
                run_meta_key,
                mapping={
                    "status": "FAILED",
                    "error": repr(e),
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                    "rows_written": str(written_rows),
                    "failed_phase": phase,
                },
            )
        except Exception:
            pass
        raise
