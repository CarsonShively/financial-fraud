import numpy as np
from sklearn.metrics import average_precision_score

def _get_positive_score(estimator, X) -> np.ndarray:
    if hasattr(estimator, "predict_proba"):
        proba = np.asarray(estimator.predict_proba(X))
        if proba.ndim != 2 or proba.shape[1] < 2:
            raise TypeError(
                f"predict_proba must return (n, 2+) for binary classification, got {proba.shape}."
            )
        return proba[:, 1]
    if hasattr(estimator, "decision_function"):
        return np.asarray(estimator.decision_function(X)).ravel()
    raise TypeError("Estimator must support predict_proba or decision_function.")

def average_precision(estimator, X, y, threshold: float = 0.5) -> float:
    return float(average_precision_score(y, _get_positive_score(estimator, X)))

def recall_at_top_1pct(estimator, X, y, threshold: float = 0.5) -> float:
    s = _get_positive_score(estimator, X)
    y_true = np.asarray(y).astype(int).ravel()

    n = y_true.shape[0]
    if n == 0:
        return 0.0

    k = max(1, int(np.ceil(n * 0.01)))
    idx = np.argsort(-s)[:k]

    total_pos = int(np.sum(y_true == 1))
    if total_pos == 0:
        return 0.0

    tp_in_topk = int(np.sum(y_true[idx] == 1))
    return float(tp_in_topk / total_pos)

def precision_at_top_1pct(estimator, X, y, threshold: float = 0.5) -> float:
    s = _get_positive_score(estimator, X)
    y_true = np.asarray(y).astype(int).ravel()

    n = y_true.shape[0]
    if n == 0:
        return 0.0

    k = max(1, int(np.ceil(n * 0.01)))
    idx = np.argsort(-s)[:k]

    tp_in_topk = int(np.sum(y_true[idx] == 1))
    return float(tp_in_topk / k)

METRICS = {
    "average_precision": average_precision,
    "recall_at_top_1pct": recall_at_top_1pct,
    "precision_at_top_1pct": precision_at_top_1pct,
}
