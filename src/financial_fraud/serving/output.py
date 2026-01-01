from __future__ import annotations

from typing import Any, Mapping
import streamlit as st

TOP_FACTOR_TEXT: dict[str, str] = {
    # ----------------------------
    # Transaction type (one-hot)
    # ----------------------------
    "cat__type_payment": "PAYMENT transactions are usually routine; when they drive the score it’s because the model is contrasting them with higher-risk patterns (like TRANSFER/CASH_OUT).",
    "cat__type_transfer": "TRANSFER is commonly used in fraud flows (moving funds quickly), so seeing a TRANSFER often pushes the model toward higher risk.",
    "cat__type_cash_out": "CASH_OUT is a classic fraud step (converting moved funds into cash), so CASH_OUT strongly signals higher risk in many fraud datasets.",
    "cat__type_debit": "DEBIT is less common than other types; when it matters, the model has learned it correlates with a different risk profile than typical transactions.",
    "cat__type_cash_in": "CASH_IN is often lower risk (adding funds rather than extracting them); if it drives the score, it’s usually because it deviates from the model’s expected pattern for this entity.",
    "cat__type_unknown": "An unknown/invalid type is unusual and can indicate dirty or unexpected data, which the model may treat as higher risk.",

    # ----------------------------
    # Amount / activity intensity (log1p)
    # ----------------------------
    "num_log1p__amount": "The transaction amount is unusually large/small relative to learned patterns; extreme amounts often increase fraud risk.",
    "num_log1p__orig_txn_count": "The sender’s prior transaction count is atypical; unusually high/low activity can be a fraud signal depending on the learned pattern.",
    "num_log1p__orig_amount_sum": "The sender’s cumulative sent amount is atypical; heavy recent sending can indicate suspicious behavior.",
    "num_log1p__dest_txn_count": "The receiver has an unusual number of transactions; high receiver activity can indicate a funnel or mule account.",
    "num_log1p__dest_amount_sum": "The receiver’s cumulative received amount is atypical; large inflows can indicate aggregation of stolen funds.",
    "num_log1p__orig_amount_mean": "The sender’s average transaction size is unusual; sudden changes in typical size can flag risk.",
    "num_log1p__dest_amount_mean": "The receiver’s average transaction size is unusual; abnormal averages can indicate suspicious receiving behavior.",

    # ----------------------------
    # Timing / recency
    # ----------------------------
    "num__step": "The event occurs at a point in the sequence where fraud was more common in the training data (time/step effects).",
    "num__orig_last_step": "The time since the sender’s last activity is unusual; sudden bursts or long gaps followed by activity can increase risk.",
    "num__dest_last_step": "The time since the receiver’s last activity is unusual; newly-active or bursty receivers can be higher risk.",

    # ----------------------------
    # Balance behavior
    # ----------------------------
    "num__orig_balance_delta_sum": "The sender’s balance changes don’t look normal over time; inconsistent or extreme balance deltas can indicate suspicious flows.",
    "num__dest_balance_delta_sum": "The receiver’s balance changes don’t look normal over time; unusual deltas can indicate laundering or mule behavior.",
}


def format_output(
    result: dict,
    *,
    threshold: float = 0.10,
) -> dict:
    proba = float(result["fraud_probability"])

    tx = result.get("transaction")
    factor = result.get("top_factor")

    is_fraud = proba >= threshold
    decision = "Fraud" if is_fraud else "Not-Fraud"

    out: dict = {
        "transaction": tx,
        "decision": decision,
        "probability_pct": f"{proba:.2%}",
    }

    if is_fraud:
        explanation = None
        if isinstance(factor, dict):
            feat = factor.get("feature")
            explanation = TOP_FACTOR_TEXT.get(feat)
            if explanation is None and isinstance(feat, str):
                explanation = f"Top driver: {feat} (no mapped explanation yet)."
        out["explanation"] = explanation

    return out


LOG_COLS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "nameDest",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "decision",
    "probability_pct",
    "explanation",
]


def make_log_row_from_out(out: Mapping[str, Any]) -> dict[str, Any]:
    tx = out.get("transaction") or {}

    row: dict[str, Any] = {
        "step": tx.get("step"),
        "type": tx.get("type"),
        "amount": tx.get("amount"),
        "nameOrig": tx.get("nameOrig"),
        "nameDest": tx.get("nameDest"),
        "oldbalanceOrg": tx.get("oldbalanceOrg"),
        "newbalanceOrig": tx.get("newbalanceOrig"),
        "oldbalanceDest": tx.get("oldbalanceDest"),
        "newbalanceDest": tx.get("newbalanceDest"),
        "decision": out.get("decision"),
        "probability_pct": out.get("probability_pct"),
        "explanation": out.get("explanation"),
    }

    for c in LOG_COLS:
        row.setdefault(c, None)

    return row


def append_log_row_local(rows: list[dict], row: Mapping[str, Any], *, max_len: int = 200) -> list[dict]:
    rows = [*rows, dict(row)]
    if len(rows) > max_len:
        rows = rows[-max_len:]
    return rows
