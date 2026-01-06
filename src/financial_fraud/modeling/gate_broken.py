import numpy as np
from sklearn.metrics import average_precision_score

def gate_broken(
    *,
    y_true,
    y_score,
    min_ap_multiple_of_prev: float = 2.0,
    min_ap_minus_prev_frac: float = 0.25,
    too_good_ap: float = 0.90,
    shuffle_tol_frac: float = 0.50,
    min_floor: float = 1e-4,
) -> dict:
    y_true = np.asarray(y_true).astype(int).ravel()
    y_score = np.asarray(y_score).ravel()

    prev = float(np.mean(y_true))
    ap_real = float(average_precision_score(y_true, y_score))
    ap_shuf = float(average_precision_score(np.random.permutation(y_true), y_score))

    min_ap = max(min_ap_multiple_of_prev * prev, prev + max(min_ap_minus_prev_frac * prev, min_floor))
    shuffle_tol = max(shuffle_tol_frac * prev, min_floor)

    pass_signal = ap_real >= min_ap
    pass_shuffle = abs(ap_shuf - prev) <= shuffle_tol
    pass_not_too_good = ap_real < too_good_ap

    ok = pass_signal and pass_shuffle and pass_not_too_good

    return {
        "ok": ok,
        "prev": prev,
        "ap_real": ap_real,
        "ap_shuf": ap_shuf,
        "min_ap_required": float(min_ap),
        "shuffle_tol": float(shuffle_tol),
        "pass_signal": pass_signal,
        "pass_shuffle": pass_shuffle,
        "pass_not_too_good": pass_not_too_good,
    }
