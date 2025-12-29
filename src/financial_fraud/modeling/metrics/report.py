from collections.abc import Callable
from financial_fraud.modeling.metrics.registry import METRICS

def project_metric_report() -> dict[str, Callable]:
    keys = [
        "average_precision",
        "roc_auc",
        "precision",
        "recall",
        "recall_at_top_1pct",
        "recall_at_top_5pct",
    ]
    return {k: METRICS[k] for k in keys}
