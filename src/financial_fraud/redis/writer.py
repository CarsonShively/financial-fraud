from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import duckdb
import redis

from financial_fraud.redis.infra import RedisConfig, make_entity_key


def _decode_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _delete_prev_live_from_index(
    r: redis.Redis,
    *,
    prefix: str,
    entity_type: str,
    index_key: str,
    scan_count: int = 2000,
    delete_batch: int = 2000,
    use_unlink: bool = True,
) -> int:
    deleted = 0
    cursor = 0
    buf: list[str] = []

    while True:
        cursor, ids_raw = r.sscan(index_key, cursor=cursor, count=scan_count)

        for x in ids_raw:
            s = _decode_str(x)
            if s:
                buf.append(s)

            if len(buf) >= delete_batch:
                keys = [make_entity_key(prefix, entity_type, entity_id) for entity_id in buf]
                pipe = r.pipeline(transaction=False)
                if use_unlink:
                    pipe.unlink(*keys)
                else:
                    pipe.delete(*keys)
                pipe.execute()
                deleted += len(buf)
                buf.clear()

        if cursor == 0:
            break

    if buf:
        keys = [make_entity_key(prefix, entity_type, entity_id) for entity_id in buf]
        pipe = r.pipeline(transaction=False)
        if use_unlink:
            pipe.unlink(*keys)
        else:
            pipe.delete(*keys)
        pipe.execute()
        deleted += len(buf)
        buf.clear()

    return deleted


def write_to_redis(
    con: duckdb.DuckDBPyConnection,
    r: redis.Redis,
    *,
    cfg: RedisConfig,
    table: str,
    entity_type: str,
    entity_col: str,
    batch_size: int = 1000,
    scan_count: int = 2000,
    delete_batch: int = 2000,
    use_unlink: bool = True,
) -> tuple[int, str]:
    started_at = datetime.now(timezone.utc).isoformat()

    live_prefix = cfg.live_prefix
    index_key = f"{live_prefix}{entity_type}:index"
    run_meta_key = f"{cfg.run_meta_prefix}LIVE:{entity_type}"

    written_rows = 0

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
            "mode": "overwrite_live_status_gated",
        },
    )

    try:
        deleted_prev = _delete_prev_live_from_index(
            r,
            prefix=live_prefix,
            entity_type=entity_type,
            index_key=index_key,
            scan_count=scan_count,
            delete_batch=delete_batch,
            use_unlink=use_unlink,
        )

        if use_unlink:
            r.unlink(index_key)
        else:
            r.delete(index_key)

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

                pipe.hset(key, mapping=mapping)
                pipe.sadd(index_key, entity_id)
                written_rows += 1

            pipe.execute()

        unique_entities = r.scard(index_key)

        r.hset(
            run_meta_key,
            mapping={
                "status": "READY",
                "rows_written": str(written_rows),
                "unique_entities": str(unique_entities),
                "deleted_prev_entities": str(deleted_prev),
                "ready_at": datetime.now(timezone.utc).isoformat(),
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
