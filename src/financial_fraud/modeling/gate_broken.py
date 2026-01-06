import numpy as np
from sklearn.metrics import average_precision_score


def gate_broken(
    *,
    y_true_hold,
    y_score_hold,
    y_true_train=None,
    y_score_train=None,
    min_ap_multiple_of_prev: float = 2.0,
    min_ap_minus_prev_frac: float = 0.25,
    too_good_ap: float = 0.90,
    shuffle_tol_frac: float = 0.50,
    max_ap_gap: float = 0.25,
    max_ap_ratio: float = 3.0,
    min_hold_ap_for_gap_check: float = 0.01,
    min_floor: float = 1e-4,
    seed: int = 0,
) -> dict:
    y_true_hold = np.asarray(y_true_hold).astype(int).ravel()
    y_score_hold = np.asarray(y_score_hold).ravel()

    prev = float(np.mean(y_true_hold))
    ap_hold = float(average_precision_score(y_true_hold, y_score_hold))

    rng = np.random.default_rng(seed)
    ap_shuf = float(
        average_precision_score(rng.permutation(y_true_hold), y_score_hold)
    )

    min_ap = max(
        min_ap_multiple_of_prev * prev,
        prev + max(min_ap_minus_prev_frac * prev, min_floor),
    )

    shuffle_tol = max(shuffle_tol_frac * prev, min_floor)

    pass_signal = ap_hold >= min_ap
    pass_shuffle = abs(ap_shuf - prev) <= shuffle_tol
    pass_not_too_good = ap_hold < too_good_ap

    ap_train = None
    ap_gap = None
    ap_ratio = None
    pass_train_hold_gap = True
    gap_check_ap_floor = max(min_hold_ap_for_gap_check, min_ap)

    if y_true_train is not None and y_score_train is not None:
        y_true_train = np.asarray(y_true_train).astype(int).ravel()
        y_score_train = np.asarray(y_score_train).ravel()

        ap_train = float(average_precision_score(y_true_train, y_score_train))
        ap_gap = ap_train - ap_hold
        ap_ratio = ap_train / max(ap_hold, min_floor)

        if ap_hold >= gap_check_ap_floor:
            pass_train_hold_gap = (ap_gap <= max_ap_gap) and (ap_ratio <= max_ap_ratio)

    ok = pass_signal and pass_shuffle and pass_not_too_good and pass_train_hold_gap

    return {
        "ok": ok,
        "prev": prev,
        "ap_hold": ap_hold,
        "ap_shuf": ap_shuf,
        "min_ap_required": float(min_ap),
        "shuffle_tol": float(shuffle_tol),
        "pass_signal": pass_signal,
        "pass_shuffle": pass_shuffle,
        "pass_not_too_good": pass_not_too_good,
        "ap_train": ap_train,
        "ap_gap_train_minus_hold": ap_gap,
        "ap_ratio_train_over_hold": ap_ratio,
        "min_hold_ap_for_gap_check": float(min_hold_ap_for_gap_check),
        "gap_check_ap_floor_used": float(gap_check_ap_floor),
        "pass_train_hold_gap": pass_train_hold_gap,
        "seed": int(seed),
    }
