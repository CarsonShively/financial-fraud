from __future__ import annotations
from typing import Mapping, Any

def delta_features(tx: Mapping[str, Any]) -> dict[str, float]:
    amount = float(tx["amount"])

    old_orig = float(tx["oldbalance_orig"])
    new_orig = float(tx["newbalance_orig"])

    old_dest = float(tx["oldbalance_dest"])
    new_dest = float(tx["newbalance_dest"])

    orig_balance_delta = old_orig - new_orig
    orig_delta_minus_amount = orig_balance_delta - amount

    dest_balance_delta = new_dest - old_dest
    dest_delta_minus_amount = dest_balance_delta - amount

    return {
        "orig_balance_delta": orig_balance_delta,
        "orig_delta_minus_amount": orig_delta_minus_amount,
        "dest_balance_delta": dest_balance_delta,
        "dest_delta_minus_amount": dest_delta_minus_amount,
    }
