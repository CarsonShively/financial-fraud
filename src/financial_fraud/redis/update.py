from __future__ import annotations

from typing import Any, Mapping
import logging
from time import perf_counter

log = logging.getLogger(__name__)

_ORIG_FIELDS = ("orig_txn_count", "orig_amount_sum", "orig_balance_delta_sum", "orig_last_step")
_DEST_FIELDS = ("dest_txn_count", "dest_amount_sum", "dest_balance_delta_sum", "dest_last_step")


def _f(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _i(x: Any) -> int | None:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def update_entities_from_tx(
    *,
    r,
    cfg,
    run_prefix: str,
    valid_tx: Mapping[str, Any],
) -> bool:
    orig_id = valid_tx.get("name_orig")
    dest_id = valid_tx.get("name_dest")
    if orig_id is None or dest_id is None:
        log.info("redis_update skip missing_entity orig=%s dest=%s", orig_id, dest_id)
        return False

    t0 = perf_counter()

    step = _i(valid_tx.get("step"))
    amount = _f(valid_tx.get("amount")) or 0.0

    obo = _f(valid_tx.get("oldbalance_orig"))
    nbo = _f(valid_tx.get("newbalance_orig"))
    obd = _f(valid_tx.get("oldbalance_dest"))
    nbd = _f(valid_tx.get("newbalance_dest"))

    orig_delta = (nbo - obo) if (obo is not None and nbo is not None) else 0.0
    dest_delta = (nbd - obd) if (obd is not None and nbd is not None) else 0.0

    base_prefix = getattr(cfg, "base_prefix", "")
    orig_key = f"{base_prefix}{run_prefix}orig:{orig_id}"
    dest_key = f"{base_prefix}{run_prefix}dest:{dest_id}"

    # Optional: very low-noise debug line (comment out if you don’t want it)
    log.debug(
        "redis_update start step=%s amount=%.2f orig=%s dest=%s prefix=%s",
        step, amount, orig_id, dest_id, run_prefix,
    )

    try:
        p = r.pipeline(transaction=True)

        p.hincrby(orig_key, "orig_txn_count", 1)
        p.hincrbyfloat(orig_key, "orig_amount_sum", float(amount))
        p.hincrbyfloat(orig_key, "orig_balance_delta_sum", float(orig_delta))
        if step is not None:
            p.hset(orig_key, "orig_last_step", int(step))

        p.hincrby(dest_key, "dest_txn_count", 1)
        p.hincrbyfloat(dest_key, "dest_amount_sum", float(amount))
        p.hincrbyfloat(dest_key, "dest_balance_delta_sum", float(dest_delta))
        if step is not None:
            p.hset(dest_key, "dest_last_step", int(step))

        p.execute()

        ms = int((perf_counter() - t0) * 1000)
        log.info(
            "redis_update ok %dms step=%s orig=%s dest=%s prefix=%s",
            ms, step, orig_id, dest_id, run_prefix,
        )
        return True

    except Exception:
        ms = int((perf_counter() - t0) * 1000)
        log.exception(
            "redis_update fail %dms step=%s orig=%s dest=%s prefix=%s",
            ms, step, orig_id, dest_id, run_prefix,
        )
        return False
