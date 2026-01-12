"""
From the read entity Dest assemble aggregate features.
"""

from __future__ import annotations

from typing import Mapping, Any
from financial_fraud.config import DEST_BUCKET_N


def _get_int(d: Mapping[str, Any], k: str, default: int = 0) -> int:
    v = d.get(k)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _get_float(d: Mapping[str, Any], k: str, default: float = 0.0) -> float:
    v = d.get(k)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def dest_aggregates(*, dest_state: Mapping[str, Any], N: int = DEST_BUCKET_N) -> dict[str, float]:
    cnt_1h = float(_get_int(dest_state, "dest_cnt_b1", 0))
    sum_1h = float(_get_float(dest_state, "dest_sum_b1", 0.0))

    cnt_24h = 0.0
    sum_24h = 0.0
    for i in range(1, N + 1):
        cnt_24h += float(_get_int(dest_state, f"dest_cnt_b{i}", 0))
        sum_24h += float(_get_float(dest_state, f"dest_sum_b{i}", 0.0))

    return {
        "dest_txn_count_1h": cnt_1h,
        "dest_txn_count_24h": cnt_24h,
        "dest_amount_sum_1h": sum_1h,
        "dest_amount_sum_24h": sum_24h,
    }
