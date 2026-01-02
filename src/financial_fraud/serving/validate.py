from __future__ import annotations

from typing import Any, Mapping


_ALLOWED_TYPES = {"payment", "transfer", "cash_out", "debit", "cash_in"}


def _try_int(x: Any) -> int | None:
    if x is None:
        return None
    try:
        s = str(x).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _try_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _norm_str(x: Any, *, lower: bool = False) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    return s.lower() if lower else s


def silver_base_row(tx: Mapping[str, Any]) -> dict[str, Any]:

    step = tx.get("step")
    type_ = tx.get("type")
    amount = tx.get("amount")
    nameOrig = tx.get("nameOrig")
    oldbalanceOrg = tx.get("oldbalanceOrg")
    newbalanceOrig = tx.get("newbalanceOrig")
    nameDest = tx.get("nameDest")
    oldbalanceDest = tx.get("oldbalanceDest")
    newbalanceDest = tx.get("newbalanceDest")

    step_t = _try_int(step)
    type_t = _norm_str(type_, lower=True)
    amount_t = _try_float(amount)

    name_orig = _norm_str(nameOrig, lower=False)
    oldbalance_orig = _try_float(oldbalanceOrg)
    newbalance_orig = _try_float(newbalanceOrig)

    name_dest = _norm_str(nameDest, lower=False)
    oldbalance_dest = _try_float(oldbalanceDest)
    newbalance_dest = _try_float(newbalanceDest)

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
