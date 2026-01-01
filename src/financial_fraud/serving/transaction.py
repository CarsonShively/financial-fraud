from __future__ import annotations

from typing import Any, Mapping
import pandas as pd

from financial_fraud.redis.reader import read_entities
from financial_fraud.serving.validate import validate_tx
from financial_fraud.serving.assemble_row import assemble_1row_df


def transaction_to_1row_df(
    tx: Mapping[str, Any],
    *,
    r,
    cfg,
    live_prefix: str,
) -> pd.DataFrame:
    valid_tx = validate_tx(tx)

    aggregates = read_entities(
        r,
        cfg=cfg,
        live_prefix=live_prefix,
        orig_id=valid_tx["name_orig"],
        dest_id=valid_tx["name_dest"],
    )

    return assemble_1row_df(valid_tx, aggregates)
