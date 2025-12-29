import numpy as np
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    precision_score,
    recall_score,
)

def _get_positive_proba(estimator, X) -> np.ndarray:
    if hasattr(estimator, "predict_proba"):
        proba = estimator.predict_proba(X)
        return proba[:, 1] if proba.ndim == 2 else np.asarray(proba).ravel()
    if hasattr(estimator, "decision_function"):
        scores = np.asarray(estimator.decision_function(X)).ravel()
        return 1.0 / (1.0 + np.exp(-scores))
    raise TypeError("Estimator must support predict_proba or decision_function.")

def _pred_from_proba(y_proba: np.ndarray, threshold: float) -> np.ndarray:
    return (y_proba >= threshold).astype(int)

def average_precision(estimator, X, y, threshold: float = 0.5) -> float:
    y_proba = _get_positive_proba(estimator, X)
    return float(average_precision_score(y, y_proba))

def precision(estimator, X, y, threshold: float = 0.5) -> float:
    y_proba = _get_positive_proba(estimator, X)
    y_pred = _pred_from_proba(y_proba, threshold)
    return float(precision_score(y, y_pred, zero_division=0))

def recall(estimator, X, y, threshold: float = 0.5) -> float:
    y_proba = _get_positive_proba(estimator, X)
    y_pred = _pred_from_proba(y_proba, threshold)
    return float(recall_score(y, y_pred, zero_division=0))

def roc_auc(estimator, X, y, threshold: float = 0.5) -> float:
    y_proba = _get_positive_proba(estimator, X)
    return float(roc_auc_score(y, y_proba))

def recall_at_top_percent(estimator, X, y, top_percent: float = 0.01) -> float:
    """
    Recall when you only action the top P% highest-risk scores (alert budget).
    Example: top_percent=0.01 means "top 1% most suspicious transactions".
    """
    if not (0.0 < top_percent <= 1.0):
        raise ValueError("top_percent must be in (0, 1].")

    y_proba = _get_positive_proba(estimator, X)
    y_true = np.asarray(y).astype(int).ravel()

    n = y_true.shape[0]
    k = max(1, int(np.ceil(n * top_percent)))

    idx = np.argsort(-y_proba)[:k]
    tp_in_topk = int(np.sum(y_true[idx] == 1))
    total_pos = int(np.sum(y_true == 1))

    return float(tp_in_topk / total_pos) if total_pos > 0 else 0.0

METRICS = {
    "average_precision": average_precision,

    "precision": precision,
    "recall": recall,

    "recall_at_top_1pct": lambda est, X, y, threshold=0.5: recall_at_top_percent(est, X, y, top_percent=0.01),
    "recall_at_top_5pct": lambda est, X, y, threshold=0.5: recall_at_top_percent(est, X, y, top_percent=0.05),


    "roc_auc": roc_auc,
}
