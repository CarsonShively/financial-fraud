from __future__ import annotations

from typing import Mapping, Any
from financial_fraud.config import DEST_BUCKET_N


def _get_int(d: Mapping[str, Any], k: str, default: int | None = None) -> int | None:
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


def dest_aggregates(
    *,
    step: int,
    dest_state: Mapping[str, Any],
    prev_last_seen_step: int | None,   # REQUIRED
    N: int = DEST_BUCKET_N,
) -> dict[str, float]:
    cnt = [int(_get_int(dest_state, f"dest_cnt_b{i}", 0) or 0) for i in range(1, N + 1)]
    amt = [float(_get_float(dest_state, f"dest_sum_b{i}", 0.0)) for i in range(1, N + 1)]

    dest_txn_count_1h = float(cnt[0])
    dest_txn_count_24h = float(sum(cnt))

    dest_amount_sum_1h = float(amt[0])
    dest_amount_sum_24h = float(sum(amt))

    dest_amount_mean_24h = dest_amount_sum_24h / dest_txn_count_24h if dest_txn_count_24h > 0 else 0.0

    if prev_last_seen_step is None:
        dest_last_gap_hours = 0.0
        dest_state_present = 0.0
    else:
        dest_last_gap_hours = float(step - prev_last_seen_step)
        dest_state_present = 1.0

    first_seen_step = _get_int(dest_state, "dest_first_seen_step", default=None)
    dest_is_warm_24h = float(
        1.0 if (first_seen_step is not None and (step - first_seen_step) >= N) else 0.0
    )

    return {
        "dest_txn_count_1h": dest_txn_count_1h,
        "dest_txn_count_24h": dest_txn_count_24h,
        "dest_amount_sum_1h": dest_amount_sum_1h,
        "dest_amount_sum_24h": dest_amount_sum_24h,
        "dest_amount_mean_24h": float(dest_amount_mean_24h),
        "dest_last_gap_hours": dest_last_gap_hours,
        "dest_state_present": dest_state_present,
        "dest_is_warm_24h": dest_is_warm_24h,
    }
