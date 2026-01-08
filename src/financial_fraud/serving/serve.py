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


# =========================
# DEBUG HELPERS (NEW)
# =========================
def _b2s(x) -> str | None:
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _hget_str(r, key: str, field: str) -> str | None:
    return _b2s(r.hget(key, field))


def _key_type_str(r, key: str) -> str:
    t = r.type(key)
    return _b2s(t) or ""


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
    # =========================
    # ORIGINAL (UNCHANGED)
    # =========================
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

    # =========================
    # DEBUG (NEW): log inputs + key sanity
    # =========================
    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            "lua_inputs dest=%s key=%s step=%s amount=%s N=%s sha_adv=%s sha_add=%s",
            dest_id,
            dest_key,
            step,
            amount,
            N,
            sha_adv,
            sha_add,
        )
        log.debug(
            "redis_key_before key=%s exists=%s type=%s",
            dest_key,
            int(r.exists(dest_key)),
            _key_type_str(r, dest_key),
        )

        # pick a few high-signal fields (adjust to match your Lua schema)
        watch_fields = [
            "dest_last_seen_step",
            "dest_prev_seen_step",
            "dest_cnt_cur",
            "dest_sum_cur",
        ]

        before = {f: _hget_str(r, dest_key, f) for f in watch_fields}
    else:
        watch_fields = []
        before = None

    # =========================
    # ORIGINAL (but wrapped): run Lua with exception logging
    # =========================
    try:
        adv_res = r.evalsha(sha_adv, 1, dest_key, step, N)
    except Exception:
        log.exception(
            "lua_adv_failed dest=%s key=%s step=%s N=%s sha=%s",
            dest_id,
            dest_key,
            step,
            N,
            sha_adv,
        )
        raise

    try:
        add_res = r.evalsha(sha_add, 1, dest_key, step, str(amount), N)
    except Exception:
        log.exception(
            "lua_add_failed dest=%s key=%s step=%s amount=%s N=%s sha=%s",
            dest_id,
            dest_key,
            step,
            amount,
            N,
            sha_add,
        )
        raise

    # =========================
    # DEBUG (NEW): diff watched fields + log return payloads
    # =========================
    if log.isEnabledFor(logging.DEBUG):
        after = {f: _hget_str(r, dest_key, f) for f in watch_fields}
        changed = {
            f: (before.get(f), after.get(f))  # type: ignore[union-attr]
            for f in watch_fields
            if before is not None and before.get(f) != after.get(f)
        }
        log.debug(
            "lua_effect dest=%s step=%s amount=%s changed=%s adv_res=%r add_res=%r",
            dest_id,
            step,
            amount,
            changed,
            adv_res,
            add_res,
        )
        log.debug(
            "redis_key_after key=%s exists=%s type=%s",
            dest_key,
            int(r.exists(dest_key)),
            _key_type_str(r, dest_key),
        )

    # =========================
    # ORIGINAL (UNCHANGED)
    # =========================
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
