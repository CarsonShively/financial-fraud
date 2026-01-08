import numpy as np

def tune_threshold(y_score, flag_rate=0.05):
    y_score = np.asarray(y_score)
    if not (0.0 < flag_rate < 1.0):
        raise ValueError("flag_rate must be in (0, 1).")
    return float(np.quantile(y_score, 1.0 - flag_rate))
