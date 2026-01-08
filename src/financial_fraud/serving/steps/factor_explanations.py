EXPLANATION_TEXT: dict[str, str] = {
    # ----------------------------
    # Transaction type
    # ----------------------------
    "cat__type_payment": (
        "Payments are usually routine. In this case, the model is comparing it "
        "to other transaction types that are more commonly linked to fraud."
    ),
    "cat__type_transfer": (
        "Transfers are often used to quickly move money between accounts, "
        "which makes them more common in fraudulent activity."
    ),
    "cat__type_cash_out": (
        "Cash-out transactions are a frequent step in fraud, where money is "
        "moved out as quickly as possible."
    ),
    "cat__type_debit": (
        "Debit transactions are less common in fraud, so when they stand out, "
        "it usually means the pattern is unusual."
    ),
    "cat__type_cash_in": (
        "Cash-in transactions are usually low risk. Here, it mattered because "
        "the behavior didn’t match what’s normally expected."
    ),
    "cat__type_unknown": (
        "An unknown or rare transaction type is unusual and can increase risk."
    ),

    # ----------------------------
    # Basic transaction info
    # ----------------------------

    "float__amount": (
        "The transaction amount is unusual compared to typical transactions."
    ),

    # ----------------------------
    # Balance behavior
    # ----------------------------
    "float__orig_balance_delta": (
        "The sender’s account balance changed in a way that doesn’t look normal."
    ),
    "float__orig_delta_minus_amount": (
        "The sender’s balance change doesn’t line up with the transaction amount, "
        "which is often a warning sign."
    ),
    "float__dest_balance_delta": (
        "The receiver’s balance changed in an unexpected way."
    ),
    "float__dest_delta_minus_amount": (
        "The receiver’s balance change doesn’t match the transaction amount, "
        "which is unusual."
    ),

    # ----------------------------
    # Destination activity
    # ----------------------------
    "int__dest_txn_count_1h": (
        "The receiving account has had an unusually high number of transactions "
        "in the last hour."
    ),
    "int__dest_txn_count_24h": (
        "The receiving account has been unusually active over the last 24 hours."
    ),
    "float__dest_amount_sum_1h": (
        "A large amount of money has been sent to this account in a short time."
    ),
    "float__dest_amount_sum_24h": (
        "This account has received an unusually large total amount over the last day."
    ),
    "float__dest_amount_mean_24h": (
        "The average transaction size for this account is higher or lower than normal."
    ),
    "float__dest_last_gap_hours": (
        "The time since this account’s previous transaction is unusual."
    ),
}
