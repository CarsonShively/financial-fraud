from __future__ import annotations

from typing import Any, Mapping
import logging
import pandas as pd
import warnings

from financial_fraud.serving.steps.base import silver_base
from financial_fraud.serving.steps.validate import validate_base
from financial_fraud.redis.reader import read_entity
from financial_fraud.serving.steps.tx_features import tx_features
from financial_fraud.serving.steps.dest_aggregates import dest_aggregates
from financial_fraud.serving.steps.delta_features import delta_features
from financial_fraud.serving.steps.explain import top_factor
from financial_fraud.serving.steps.factor_explanations import EXPLANATION_TEXT
from financial_fraud.redis.infra import make_entity_key

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

    N = int(cfg.dest_bucket_N)
    dest_key = make_entity_key(cfg.live_prefix, "dest", dest_id)

    sha_adv = lua_shas["dest_advance"]
    sha_add = lua_shas["dest_add"]

    try:
        r.evalsha(sha_adv, 1, dest_key, step, N)
    except Exception:
        log.exception(
            "lua_adv_failed dest=%s key=%s step=%s N=%s sha=%s",
            dest_id, dest_key, step, N, sha_adv,
        )
        raise

    dest_state = read_entity(r, cfg=cfg, dest_id=dest_id)
    dest = dest_aggregates(dest_state=dest_state, N=N)

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
                explanation = "Flagged — explanation unavailable for this model type."
        except Exception:
            log.exception("explain_failed dest=%s", dest_id)
            explanation = "Flagged — explanation missing for top factor."

    out = {
        "tx": dict(tx),
        "decision": decision,
        "proba": proba,
        "explanation": explanation,
    }
    audit_log = pd.DataFrame([out]).reindex(columns=["decision", "proba", "explanation", "tx"])

    try:
        r.evalsha(sha_add, 1, dest_key, step, amount, N)
    except Exception:
        log.exception(
            "lua_add_failed dest=%s key=%s step=%s amount=%s N=%s sha=%s",
            dest_id, dest_key, step, amount, N, sha_add,
        )
        raise

    return out, audit_log
