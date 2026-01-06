from collections.abc import Callable
from financial_fraud.modeling.metrics.registry import METRICS

def project_metric_report() -> dict[str, Callable]:
    keys = [
        "average_precision",
        "recall_at_top_1pct",
        "precision_at_top_1pct",
    ]
    return {k: METRICS[k] for k in keys}
