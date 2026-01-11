import numpy as np
import shap
import warnings

def top_factor_explainer(pipe):
    spec = pipe.named_steps["spec"]
    pre = pipe.named_steps["pre"]
    clf = pipe.named_steps["clf"]

    names = list(pre.get_feature_names_out())
    explainer = shap.TreeExplainer(clf)
    return spec, pre, names, explainer

def top_factor(spec, pre, names, explainer, X_row):
    X_row2 = spec.transform(X_row)
    X_t = pre.transform(X_row2)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="LightGBM binary classifier with TreeExplainer shap values output has changed to a list of ndarray",
            category=UserWarning,
        )
        sv = explainer.shap_values(X_t)
    sv_row = sv[1][0] if isinstance(sv, list) else sv[0]

    i = int(np.argmax(np.abs(sv_row)))
    row_vals = (
        X_t[0].toarray().ravel()
        if hasattr(X_t[0], "toarray")
        else np.asarray(X_t[0]).ravel()
    )
    return {
        "feature": names[i],
        "value": float(row_vals[i]),
        "contribution": float(sv_row[i]),
    }