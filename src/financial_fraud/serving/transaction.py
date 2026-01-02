from __future__ import annotations

from typing import Any, Mapping
import logging
from time import perf_counter
import pandas as pd

from financial_fraud.redis.reader import read_entities
from financial_fraud.serving.validate import silver_base_row
from financial_fraud.serving.gate import gate_tx
from financial_fraud.serving.assemble_row import assemble_1row_df

log = logging.getLogger(__name__)


def _ms(t0: float) -> int:
    return int((perf_counter() - t0) * 1000)


def transaction_to_1row_df(
    tx: Mapping[str, Any],
    *,
    r,
    cfg,
) -> tuple[pd.DataFrame, Mapping[str, Any]] | None:
    t_all = perf_counter()

    t0 = perf_counter()
    valid_tx = silver_base_row(tx)
    log.info("tx.base_row %dms step=%s", _ms(t0), valid_tx.get("step"))

    t0 = perf_counter()
    ok, rej = gate_tx(valid_tx, required=("name_orig", "name_dest"))
    log.info("tx.gate %dms ok=%s", _ms(t0), ok)

    if not ok:
        log.info(
            "tx.skip %dms reason=%s missing=%s null=%s orig=%s dest=%s",
            _ms(t_all),
            rej.reason if rej else None,
            rej.missing if rej else (),
            rej.null_fields if rej else (),
            valid_tx.get("name_orig"),
            valid_tx.get("name_dest"),
        )
        return None

    orig_id = valid_tx["name_orig"]
    dest_id = valid_tx["name_dest"]

    t0 = perf_counter()
    aggregates = read_entities(
        r,
        cfg=cfg,
        orig_id=orig_id,
        dest_id=dest_id,
    )
    orig_found = bool(aggregates.get("orig"))
    dest_found = bool(aggregates.get("dest"))
    log.info("tx.read_entities %dms orig_found=%s dest_found=%s", _ms(t0), orig_found, dest_found)

    t0 = perf_counter()
    X = assemble_1row_df(valid_tx, aggregates)
    log.info("tx.assemble %dms cols=%d", _ms(t0), len(X.columns))

    log.info("tx.done %dms step=%s orig=%s dest=%s", _ms(t_all), valid_tx.get("step"), orig_id, dest_id)
    return X, valid_tx
