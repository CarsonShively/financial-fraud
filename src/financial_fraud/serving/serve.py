from __future__ import annotations

from typing import Any, Mapping
import logging
import pandas as pd

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


def _parse_prev_last_seen(raw) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if raw == "":
        return None
    try:
        return int(raw)
    except Exception:
        return None


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

    r.evalsha(sha_adv, 1, dest_key, step, N)

    add_res = r.evalsha(sha_add, 1, dest_key, step, str(amount), N)
    if not (isinstance(add_res, (list, tuple)) and len(add_res) >= 2):
        raise RuntimeError(f"dest_add did not return expected payload, got: {add_res!r}")

    prev_last_seen_step = _parse_prev_last_seen(add_res[1])

    dest_state = read_entity(r, cfg=cfg, dest_id=dest_id)

    dest = dest_aggregates(
        step=step,
        dest_state=dest_state,
        prev_last_seen_step=prev_last_seen_step,
        N=N,
    )
    



    transaction = tx_features(base)
    delta = delta_features(base)

    row: dict[str, Any] = {**transaction, **delta, **dest}
    X = pd.DataFrame([row])

    if log.isEnabledFor(logging.INFO):
        row0 = X.iloc[0].to_dict()
        msg = "\n".join(f"{k}={row0[k]!r}" for k in sorted(row0))
        log.info("features:\n%s", msg)

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
                    feat,
                    "Multiple risk signals contributed to this decision.",
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
    return out, audit_log
