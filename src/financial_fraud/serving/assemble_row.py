from __future__ import annotations

from typing import Any, Mapping
import logging
import pandas as pd

logger = logging.getLogger(__name__)

EXPECTED_AGGS: tuple[str, ...] = (
    "orig_txn_count",
    "orig_amount_sum",
    "orig_amount_mean",
    "orig_last_step",
    "orig_balance_delta_sum",
    "dest_txn_count",
    "dest_amount_sum",
    "dest_amount_mean",
    "dest_last_step",
    "dest_balance_delta_sum",
)

def assemble_1row_df(
    valid_tx: Mapping[str, Any],
    aggregates: Mapping[str, Mapping[str, Any]],
) -> pd.DataFrame:
    base = {k: valid_tx[k] for k in ("step", "type", "amount", "name_orig", "name_dest")}

    fill: dict[str, Any] = {k: 0 for k in EXPECTED_AGGS}
    fill["orig_last_step"] = -1
    fill["dest_last_step"] = -1

    provided: set[str] = set()

    for k, v in aggregates.get("orig", {}).items():
        if k in fill:
            fill[k] = v
            provided.add(k)
    for k, v in aggregates.get("dest", {}).items():
        if k in fill:
            fill[k] = v
            provided.add(k)

    total = len(EXPECTED_AGGS)
    missing = total - len(provided)

    if missing:
        logger.info(
            "AGG FILL: %d/%d missing from Redis (orig=%s dest=%s)",
            missing, total, valid_tx.get("name_orig"), valid_tx.get("name_dest")
        )
        missing_keys = [k for k in EXPECTED_AGGS if k not in provided]
        logger.debug("Missing keys: %s", missing_keys)

    row = {**base, **fill}
    return pd.DataFrame([row])
