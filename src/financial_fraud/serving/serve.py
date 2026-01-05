from __future__ import annotations

from typing import Any, Mapping
import pandas as pd

from financial_fraud.serving.steps.base import silver_base
from financial_fraud.serving.steps.validate import validate_base
from financial_fraud.redis.reader import read_entity
from financial_fraud.serving.steps.tx_features import tx_features
from financial_fraud.serving.steps.dest_aggregates import dest_aggregates
from financial_fraud.serving.steps.delta_features import delta_features
from financial_fraud.serving.steps.explain import top_factor
from financial_fraud.serving.steps.factor_explanations import EXPLANATION_TEXT
from financial_fraud.serving.steps.lua import update_dest_aggregates

def serve(
    tx: Mapping[str, Any],
    *,
    r,
    cfg,
    model,
    explainer_bundle,
    lua_shas: dict[str, str],
) -> dict[str, Any] | None:
    base = silver_base(tx)
    
    if not validate_base(base): return None
    
    transaction = tx_features(base)

    delta = delta_features(base)
    
    dest_id = base["name_dest"]
    dest_state = read_entity(r, cfg=cfg, dest_id=dest_id)
    dest = dest_aggregates(step=base["step"], dest_state=dest_state)
    
    row: dict[str, Any] = {**transaction, **delta, **dest}
    X = pd.DataFrame([row])
    
    proba = float(model.predict_proba(X)[0, 1])
    
    factor = None
    decision = False
    explanation = "No elevated risk signals detected."
    if proba >= 0.5:
        decision = True
        spec, pre, names, explainer = explainer_bundle
        factor = top_factor(spec, pre, names, explainer, X)
        if factor in EXPLANATION_TEXT:
            explanation = EXPLANATION_TEXT[factor]
        else:
            explanation = "Multiple risk signals contributed to this decision."
        
    out = {
        "tx": dict(tx),
        "decision": decision,
        "proba": proba,
        "explanation": explanation,
    }
    
    log = pd.DataFrame([out]).reindex(
        columns=["decision", "proba", "explanation", "tx"]
    )
    
    update_dest_aggregates(
        r,
        cfg=cfg,
        sha_map=lua_shas,
        dest_id=dest_id,
        step=int(base["step"]),
        amount=float(base["amount"]),
        N=int(cfg.dest_bucket_N),
    )

    return out, log