from __future__ import annotations
from typing import Any, Mapping

TX_BASE_COLS = ("step", "type", "amount", "name_orig", "name_dest")

def tx_features(base: Mapping[str, Any]) -> dict[str, Any]:
    return {k: base[k] for k in TX_BASE_COLS}
