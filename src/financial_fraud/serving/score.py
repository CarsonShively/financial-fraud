from __future__ import annotations

from time import perf_counter
import logging

from financial_fraud.serving.transaction import transaction_to_1row_df
from financial_fraud.serving.explain import top_factor_fraud
from financial_fraud.redis.update import update_entities_from_tx

log = logging.getLogger(__name__)


def _ms(t0: float) -> int:
    return int((perf_counter() - t0) * 1000)


def score_transaction(
    *,
    tx,
    r,
    cfg,
    model,
    explainer_bundle,
) -> dict | None:
    t_all = perf_counter()

    t0 = perf_counter()
    bundle = transaction_to_1row_df(tx, r=r, cfg=cfg)
    log.info("score.build_row %dms ok=%s", _ms(t0), bundle is not None)

    if bundle is None:
        log.info("score.done %dms skipped=invalid_entity", _ms(t_all))
        return None

    X, valid_tx = bundle
    step = valid_tx.get("step")
    orig = valid_tx.get("name_orig")
    dest = valid_tx.get("name_dest")

    t0 = perf_counter()
    proba = float(model.predict_proba(X)[0, 1])
    log.info("score.predict %dms step=%s p=%.4f", _ms(t0), step, proba)

    t0 = perf_counter()
    spec, pre, names, explainer = explainer_bundle
    factor = top_factor_fraud(
        spec=spec,
        pre=pre,
        names=names,
        explainer=explainer,
        X_row=X,
    )
    log.info("score.explain %dms step=%s factor=%s", _ms(t0), step, factor)

    t0 = perf_counter()
    updated = update_entities_from_tx(
        r=r,
        cfg=cfg,
        run_prefix="LIVE:",
        valid_tx=valid_tx,
    )
    log.info("score.update_entities %dms step=%s ok=%s orig=%s dest=%s", _ms(t0), step, updated, orig, dest)

    log.info("score.done %dms step=%s", _ms(t_all), step)

    return {
        "transaction": dict(tx) if hasattr(tx, "items") else tx,
        "fraud_probability": proba,
        "top_factor": factor,
    }
