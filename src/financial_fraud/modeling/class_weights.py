from __future__ import annotations

import numpy as np

def compute_scale_pos_weight(y) -> float:
    y = np.asarray(y)
    pos = float((y == 1).sum())
    neg = float((y == 0).sum())
    if pos == 0:
        raise ValueError("No positive samples in y; cannot compute scale_pos_weight.")
    return neg / pos
