"""
Take a transaction in run it through online pipeline and return the correctly formated log for that transaction.
"""

from __future__ import annotations

from typing import Any, Mapping
import logging
import pandas as pd
import warnings

from financial_fraud.serving.steps.base import silver_base
from financial_fraud.serving.steps.validate import validate_base
from financial_fraud.serving.steps.tx_features import tx_features
from financial_fraud.serving.steps.delta_features import delta_features
from financial_fraud.serving.steps.explain import top_factor
from financial_fraud.serving.steps.factor_explanations import EXPLANATION_TEXT
from financial_fraud.serving.steps.entity_features import get_entity_features

log = logging.getLogger(__name__)


def serve(
    tx: Mapping[str, Any],
    *,
    r,
    cfg,
    model,
    threshold: float | None = None,
    explainer_bundle=None,
    lua_shas: dict[str, str],
) -> tuple[dict[str, Any], pd.DataFrame] | None:
    base = silver_base(tx)
    if not validate_base(base):
        return None

    step = int(base["step"])
    amount = float(base["amount"])
    dest_id = base["name_dest"]

    dest = get_entity_features(
        r=r,
        cfg=cfg,
        lua_shas=lua_shas,
        dest_id=dest_id,
        step=step,
        amount=amount,
    )

    transaction = tx_features(base)
    delta = delta_features(base)

    row: dict[str, Any] = {**transaction, **delta, **dest}
    X = pd.DataFrame([row])

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*does not have valid feature names.*",
            category=UserWarning,
        )
        proba = float(model.predict_proba(X)[0, 1])

    decision = False
    explanation = "No elevated risk signals detected."
    if threshold is not None and proba >= threshold:
        decision = True
        try:
            if explainer_bundle is not None:
                spec, pre, names, explainer = explainer_bundle
                factor = top_factor(spec, pre, names, explainer, X)
                feat = factor.get("feature") if isinstance(factor, dict) else None
                explanation = EXPLANATION_TEXT.get(
                    feat, "Multiple risk signals contributed to this decision."
                )
            else:
                explanation = "Flagged - explanation unavailable for this model type."
        except Exception:
            log.exception("explain_failed dest=%s", dest_id)
            explanation = "Flagged - explanation missing for top factor."

    out = {
        "tx": dict(tx),
        "decision": decision,
        "proba": proba,
        "explanation": explanation,
    }
    audit_log = pd.DataFrame([out]).reindex(columns=["decision", "proba", "explanation", "tx"])

    return out, audit_log
