from __future__ import annotations

from financial_fraud.serving.transaction import transaction_to_1row_df
from financial_fraud.serving.explain import top_factor_fraud


def score_transaction(
    *,
    tx,
    r,
    cfg,
    run_prefix: str,
    model,
    explainer_bundle,
) -> dict:
    X = transaction_to_1row_df(tx, r=r, cfg=cfg, live_prefix=run_prefix)

    proba = float(model.predict_proba(X)[0, 1])

    spec, pre, names, explainer = explainer_bundle
    factor = top_factor_fraud(
        spec=spec,
        pre=pre,
        names=names,
        explainer=explainer,
        X_row=X,
    )

    return {
        "transaction": dict(tx) if hasattr(tx, "items") else tx,
        "fraud_probability": proba,
        "top_factor": factor,
    }
