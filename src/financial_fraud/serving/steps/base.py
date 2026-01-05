from __future__ import annotations

from typing import Any, Mapping
import math

_ALLOWED_TYPES = {"payment", "transfer", "cash_out", "debit", "cash_in"}


def _nullif_blank_str(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return None if s == "" else s


def _try_float_nullable(x: Any) -> float | None:
    s = _nullif_blank_str(x)
    if s is None:
        return None
    try:
        v = float(s)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _try_int_coerce_nullable(x: Any) -> int | None:
    v = _try_float_nullable(x)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def silver_base(tx: Mapping[str, Any]) -> dict[str, Any]:
    step = tx.get("step")
    type_ = tx.get("type")
    amount = tx.get("amount")
    nameOrig = tx.get("nameOrig")
    oldbalanceOrg = tx.get("oldbalanceOrg")
    newbalanceOrig = tx.get("newbalanceOrig")
    nameDest = tx.get("nameDest")
    oldbalanceDest = tx.get("oldbalanceDest")
    newbalanceDest = tx.get("newbalanceDest")

    step_t = _try_int_coerce_nullable(step)

    type_s = _nullif_blank_str(type_)
    type_t = type_s.lower() if type_s is not None else None

    amount_t = _try_float_nullable(amount)

    name_orig = _nullif_blank_str(nameOrig)
    name_dest = _nullif_blank_str(nameDest)

    oldbalance_orig = _try_float_nullable(oldbalanceOrg)
    newbalance_orig = _try_float_nullable(newbalanceOrig)
    oldbalance_dest = _try_float_nullable(oldbalanceDest)
    newbalance_dest = _try_float_nullable(newbalanceDest)

    if type_t not in _ALLOWED_TYPES:
        type_t = None

    if step_t is None or step_t < 0:
        step_t = None
    if amount_t is None or amount_t < 0:
        amount_t = None

    if oldbalance_orig is None or oldbalance_orig < 0:
        oldbalance_orig = None
    if newbalance_orig is None or newbalance_orig < 0:
        newbalance_orig = None
    if oldbalance_dest is None or oldbalance_dest < 0:
        oldbalance_dest = None
    if newbalance_dest is None or newbalance_dest < 0:
        newbalance_dest = None

    return {
        "step": step_t,
        "type": type_t,
        "amount": amount_t,
        "name_orig": name_orig,
        "oldbalance_orig": oldbalance_orig,
        "newbalance_orig": newbalance_orig,
        "name_dest": name_dest,
        "oldbalance_dest": oldbalance_dest,
        "newbalance_dest": newbalance_dest,
    }
