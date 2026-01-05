from __future__ import annotations
from typing import Mapping, Any

N_BUCKETS = 24

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

def dest_aggregates(*, step: int, dest_state: Mapping[str, Any]) -> dict[str, float]:
    cnt = [_get_int(dest_state, f"dest_cnt_b{i}", 0) for i in range(1, 25)]
    amt = [_get_float(dest_state, f"dest_sum_b{i}", 0.0) for i in range(1, 25)]

    dest_txn_count_1h = float(cnt[0])
    dest_txn_count_24h = float(sum(cnt))

    dest_amount_sum_1h = float(amt[0])
    dest_amount_sum_24h = float(sum(amt))

    dest_amount_mean_24h = dest_amount_sum_24h / dest_txn_count_24h if dest_txn_count_24h > 0 else 0.0

    last_seen_step = _get_int(dest_state, "dest_last_seen_step", default=-1)
    dest_last_gap_hours = float(step - last_seen_step) if last_seen_step >= 0 else 10000.0

    return {
        "dest_txn_count_1h": dest_txn_count_1h,
        "dest_txn_count_24h": dest_txn_count_24h,
        "dest_amount_sum_1h": dest_amount_sum_1h,
        "dest_amount_sum_24h": dest_amount_sum_24h,
        "dest_amount_mean_24h": float(dest_amount_mean_24h),
        "dest_last_gap_hours": dest_last_gap_hours,
    }
