from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import logging
from time import perf_counter

import duckdb
import redis

from financial_fraud.redis.infra import RedisConfig, make_entity_key

log = logging.getLogger(__name__)


def _decode_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _ms(t0: float) -> int:
    return int((perf_counter() - t0) * 1000)


def _log_redis_mem(r: redis.Redis, *, label: str) -> None:
    """
    Log a compact, readable snapshot of Redis memory.
    Focus on RSS + fragmentation because OOM-killer cares about RSS.
    """
    try:
        m = r.info("memory")
        log.info(
            "redis_mem %s used=%s rss=%s frag=%s peak=%s",
            label,
            m.get("used_memory_human"),
            m.get("used_memory_rss_human"),
            m.get("mem_fragmentation_ratio"),
            m.get("used_memory_peak_human"),
        )
    except Exception:
        # Don't let logging break the backfill.
        log.debug("redis_mem %s (failed to read info memory)", label, exc_info=True)


def _delete_prev_live_by_scan(
    r: redis.Redis,
    *,
    prefix: str,
    entity_type: str,
    scan_count: int = 2000,
    delete_batch: int = 2000,
    use_unlink: bool = True,
) -> int:
    """
    Delete previous LIVE keys by scanning the keyspace instead of keeping a giant index set.
    Much lower Redis memory usage (no per-entity SADD index).
    """
    pattern = f"{prefix}{entity_type}:*"

    deleted = 0
    cursor = 0
    buf: list[str] = []

    t_all = perf_counter()
    log.info(
        "redis_backfill.delete_prev_scan start entity_type=%s pattern=%s",
        entity_type,
        pattern,
    )

    while True:
        t0 = perf_counter()
        cursor, keys_raw = r.scan(cursor=cursor, match=pattern, count=scan_count)
        log.debug(
            "redis_backfill.scan %dms cursor=%s n=%d",
            _ms(t0),
            cursor,
            len(keys_raw),
        )

        for k in keys_raw:
            s = _decode_str(k)
            if s:
                buf.append(s)

            if len(buf) >= delete_batch:
                t_del = perf_counter()
                pipe = r.pipeline(transaction=False)
                if use_unlink:
                    pipe.unlink(*buf)
                else:
                    pipe.delete(*buf)
                pipe.execute()
                deleted += len(buf)
                buf.clear()
                log.info("redis_backfill.delete_batch %dms deleted=%d", _ms(t_del), deleted)

        if cursor == 0:
            break

    if buf:
        t_del = perf_counter()
        pipe = r.pipeline(transaction=False)
        if use_unlink:
            pipe.unlink(*buf)
        else:
            pipe.delete(*buf)
        pipe.execute()
        deleted += len(buf)
        buf.clear()
        log.info("redis_backfill.delete_batch %dms deleted=%d", _ms(t_del), deleted)

    log.info("redis_backfill.delete_prev_scan done %dms deleted=%d", _ms(t_all), deleted)
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
    log_memory: bool = True,
    mem_every_batches: int = 50,
) -> tuple[int, str]:
    started_at = datetime.now(timezone.utc).isoformat()

    live_prefix = cfg.live_prefix
    run_meta_key = f"{cfg.run_meta_prefix}LIVE:{entity_type}"

    written_rows = 0
    phase = "init"
    t_all = perf_counter()

    log.info(
        "redis_backfill.start table=%s entity_type=%s entity_col=%s batch=%d prefix=%s",
        table, entity_type, entity_col, batch_size, live_prefix,
    )
    if log_memory:
        _log_redis_mem(r, label=f"{entity_type}.start")

    r.hset(
        run_meta_key,
        mapping={
            "status": "WRITING",
            "run_prefix": live_prefix,
            "source_table": table,
            "entity_type": entity_type,
            "entity_col": entity_col,
            "started_at": started_at,
            "mode": "overwrite_live_scan_delete",
        },
    )

    deleted_prev: int = 0

    try:
        phase = "delete_prev_scan"
        t0 = perf_counter()
        deleted_prev = _delete_prev_live_by_scan(
            r,
            prefix=live_prefix,
            entity_type=entity_type,
            scan_count=scan_count,
            delete_batch=delete_batch,
            use_unlink=use_unlink,
        )
        log.info("redis_backfill.delete_prev_phase %dms deleted_prev=%d", _ms(t0), deleted_prev)
        if log_memory:
            _log_redis_mem(r, label=f"{entity_type}.after_delete")

        phase = "duckdb_execute"
        t0 = perf_counter()
        cur = con.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        log.info("redis_backfill.duckdb_execute %dms cols=%d", _ms(t0), len(cols))

        if entity_col not in cols:
            raise ValueError(f"{table} must include entity column '{entity_col}'")

        entity_idx = cols.index(entity_col)

        phase = "write_batches"
        batch_no = 0
        while True:
            t_fetch = perf_counter()
            rows = cur.fetchmany(batch_size)
            log.debug("redis_backfill.fetchmany %dms n=%d", _ms(t_fetch), len(rows))

            if not rows:
                break

            batch_no += 1
            t_batch = perf_counter()
            pipe = r.pipeline(transaction=False)

            batch_written = 0
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
                batch_written += 1

            t_exec = perf_counter()
            pipe.execute()
            log.info(
                "redis_backfill.batch %dms batch_no=%d wrote=%d total=%d exec=%dms",
                _ms(t_batch), batch_no, batch_written, written_rows, _ms(t_exec),
            )

            if log_memory and mem_every_batches > 0 and (batch_no % mem_every_batches == 0):
                _log_redis_mem(r, label=f"{entity_type}.batch{batch_no}")

        phase = "finalize_counts"
        t0 = perf_counter()
        unique_entities = int(
            con.execute(f"SELECT COUNT(DISTINCT {entity_col}) FROM {table}").fetchone()[0]
        )
        log.info("redis_backfill.unique_duckdb %dms unique=%d", _ms(t0), unique_entities)

        phase = "meta_ready"
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

        if log_memory:
            _log_redis_mem(r, label=f"{entity_type}.done")

        log.info(
            "redis_backfill.done %dms rows_written=%d unique=%d deleted_prev=%d",
            _ms(t_all), written_rows, unique_entities, deleted_prev,
        )
        return written_rows, live_prefix

    except Exception as e:
        log.exception(
            "redis_backfill.fail phase=%s rows_written=%d table=%s entity_type=%s entity_col=%s",
            phase, written_rows, table, entity_type, entity_col,
        )
        if log_memory:
            _log_redis_mem(r, label=f"{entity_type}.FAILED.{phase}")

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
            log.exception("redis_backfill.fail_meta_write phase=%s", phase)
        raise
