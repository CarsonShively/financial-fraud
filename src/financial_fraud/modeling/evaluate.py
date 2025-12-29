from typing import Callable, Dict, Any
from sklearn.pipeline import Pipeline

MetricFn = Callable[[Pipeline, Any, Any, float], float]

def evaluate(artifact: Pipeline, X, y, *, metrics: Dict[str, MetricFn], threshold: float = 0.5) -> dict:
    return {name: float(fn(artifact, X, y, threshold)) for name, fn in metrics.items()}