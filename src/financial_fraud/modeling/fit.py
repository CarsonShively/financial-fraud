from typing import Callable, Any
import numpy as np
from sklearn.pipeline import Pipeline

from financial_fraud.modeling.class_weights import compute_scale_pos_weight


def fit_pipeline(
    *,
    build_pipeline: Callable[[], Pipeline],
    X,
    y,
) -> tuple[Pipeline, list[str] | None]:
    pipe = build_pipeline()

    y_arr = np.asarray(y)
    spw = compute_scale_pos_weight(y_arr)

    sample_weight = np.ones_like(y_arr, dtype=float)
    sample_weight[y_arr == 1] = spw

    pipe.fit(X, y_arr, clf__sample_weight=sample_weight)

    pre = pipe.named_steps.get("pre")
    feature_names: list[str] | None = None
    if pre is not None and hasattr(pre, "get_feature_names_out"):
        feature_names = pre.get_feature_names_out().tolist()

    return pipe, feature_names
