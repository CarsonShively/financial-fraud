from __future__ import annotations
from typing import Any, Mapping

def validate_tx(tx: Mapping[str, Any]) -> dict[str, Any]:
    key_map = {
        "step": "step",
        "type": "type",
        "amount": "amount",
        "nameOrig": "name_orig",
        "oldbalanceOrg": "oldbalance_orig",
        "newbalanceOrig": "newbalance_orig",
        "nameDest": "name_dest",
        "oldbalanceDest": "oldbalance_dest",
        "newbalanceDest": "newbalance_dest",
        "name_orig": "name_orig",
        "oldbalance_orig": "oldbalance_orig",
        "newbalance_orig": "newbalance_orig",
        "name_dest": "name_dest",
        "oldbalance_dest": "oldbalance_dest",
        "newbalance_dest": "newbalance_dest",
    }

    required_out = [
        "step",
        "type",
        "amount",
        "name_orig",
        "oldbalance_orig",
        "newbalance_orig",
        "name_dest",
        "oldbalance_dest",
        "newbalance_dest",
    ]

    out: dict[str, Any] = {}
    for k, v in tx.items():
        if k in key_map:
            out[key_map[k]] = v

    missing = [k for k in required_out if k not in out]
    if missing:
        raise ValueError(f"Missing required field(s) after normalization: {', '.join(missing)}")

    return out
