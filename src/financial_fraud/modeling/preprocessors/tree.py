from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer


def preprocessor() -> ColumnTransformer:
    type_categories = [["payment", "transfer", "cash_out", "debit", "cash_in", "unknown"]]

    CAT_COLS = ["type"]

    ZERO_NUM_COLS = [
        "amount",
        "orig_balance_delta",
        "orig_delta_minus_amount",
        "dest_balance_delta",
        "dest_delta_minus_amount",
        "dest_txn_count_1h",
        "dest_txn_count_24h",
        "dest_amount_sum_1h",
        "dest_amount_sum_24h",
        "dest_amount_mean_24h",
        "dest_last_gap_hours",
        "dest_state_present",
        "dest_is_warm_24h",
    ]

    ohe_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
            ("ohe", OneHotEncoder(
                categories=type_categories,
                handle_unknown="ignore",
                sparse_output=False,
            )),
        ]
    )

    num_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("cat", ohe_pipeline, CAT_COLS),
            ("num", num_pipeline, ZERO_NUM_COLS),
        ],
        remainder="drop",
        verbose_feature_names_out=True,
    )
