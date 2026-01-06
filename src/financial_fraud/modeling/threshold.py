import numpy as np
from sklearn.metrics import recall_score

def tune_threshold(y_true, y_score, min_recall=0.80, grid=None):
    if grid is None:
        grid = np.linspace(0.99, 0.01, 99)

    best = None
    for t in grid:
        y_pred = (y_score >= t).astype(int)
        r = recall_score(y_true, y_pred, zero_division=0)
        if r >= min_recall:
            best = t
            break

    return float(best) if best is not None else None
