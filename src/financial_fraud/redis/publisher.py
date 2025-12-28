from __future__ import annotations

from typing import Iterable

import redis

from financial_fraud.redis.infra import RedisConfig, make_entity_key


def _decode_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _chunked(it: Iterable[str], n: int) -> Iterable[list[str]]:
    buf: list[str] = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def delete_version_prefix(
    r: redis.Redis,
    *,
    prefix: str,
    entity_types: tuple[str, ...] = ("orig", "dest"),
    delete_batch: int = 2000,
) -> int:
    """
    Deletes one version prefix using the per-version index sets:
      {prefix}{entity_type}:index -> IDs
      make_entity_key(prefix, entity_type, entity_id) -> entity hash key
    """
    deleted = 0
    for et in entity_types:
        index_key = f"{prefix}{et}:index"
        ids_raw = r.smembers(index_key)
        ids = {_decode_str(x) for x in ids_raw if _decode_str(x)}

        if ids:
            for chunk in _chunked(sorted(ids), delete_batch):
                pipe = r.pipeline(transaction=False)
                for entity_id in chunk:
                    pipe.delete(make_entity_key(prefix, et, entity_id))
                pipe.execute()
                deleted += len(chunk)

        r.delete(index_key)

    return deleted


def publish_keep_one(
    r: redis.Redis,
    *,
    cfg: RedisConfig,
    new_prefix: str,
) -> str:
    """
    Staging-only publish:
      - Flip CURRENT to new_prefix
      - Return old CURRENT prefix (caller may delete it)

    No PREVIOUS pointer is used or written.
    """
    old_current = _decode_str(r.get(cfg.current_pointer_key))
    r.set(cfg.current_pointer_key, new_prefix)
    return old_current
