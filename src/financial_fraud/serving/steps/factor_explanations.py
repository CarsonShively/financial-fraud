EXPLANATION_TEXT: dict[str, str] = {
    # ----------------------------
    # Transaction type
    # ----------------------------
    "cat__type_payment": "Payment transactions are typically lower risk than other types.",
    "cat__type_transfer": "Transfers are commonly used to move funds quickly in fraud patterns.",
    "cat__type_cash_out": "Cash-out activity often appears when fraudsters try to extract funds.",
    "cat__type_debit": "Debit-type activity can be higher risk depending on the surrounding pattern.",
    "cat__type_cash_in": "Cash-in activity is usually lower risk and can be used as a contrast signal.",
    "cat__type_unknown": "An unknown transaction type increases uncertainty and risk.",

    # ----------------------------
    # Basic transaction info
    # ----------------------------
    "num__amount": "The transaction amount is unusually large or atypical.",

    # ----------------------------
    # Balance behavior
    # ----------------------------
    "num__orig_balance_delta": "The sender’s balance changed sharply.",
    "num__orig_delta_minus_amount": "The sender’s balance change doesn’t match the transaction amount.",
    "num__dest_balance_delta": "The recipient’s balance shifted unexpectedly.",
    "num__dest_delta_minus_amount": "The recipient’s balance change doesn’t align with the transfer amount.",

    # ----------------------------
    # Destination activity
    # ----------------------------
    "num__dest_txn_count_1h": "The recipient received many transactions in the last hour.",
    "num__dest_txn_count_24h": "The recipient shows unusually high activity over 24 hours.",
    "num__dest_amount_sum_1h": "A large total value flowed to this recipient recently.",
    "num__dest_amount_sum_24h": "The recipient accumulated a high total amount today.",
    "num__dest_amount_mean_24h": "The recipient’s average transaction size is unusually high.",
    "num__dest_last_gap_hours": "The time since the recipient’s last transaction is unusual.",

    # ----------------------------
    # Destination state / warm start
    # ----------------------------
    "num__dest_state_present": "This recipient has recent history available in the feature store.",
    "num__dest_is_warm_24h": "This recipient has been active recently, which increases risk context.",
}

