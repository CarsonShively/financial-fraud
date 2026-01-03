import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer


def preprocessor() -> ColumnTransformer:
    type_categories = [["payment", "transfer", "cash_out", "debit", "cash_in", "unknown"]]

    OHE_COLS = [
        "type",
    ]

    IMPUTE_INT_COLS = [
        "step",
        "dest_txn_count_1h",
        "dest_txn_count_24h",
    ]

    IMPUTE_FLOAT_COLS = [
        "amount",
        "orig_balance_delta",
        "orig_delta_minus_amount",
        "dest_balance_delta",
        "dest_delta_minus_amount",
        "dest_amount_sum_1h",
        "dest_amount_sum_24h",
        "dest_amount_mean_24h",
        "dest_last_gap_hours",
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

    int_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
        ]
    )

    float_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("cat", ohe_pipeline, OHE_COLS),
            ("int", int_pipeline, IMPUTE_INT_COLS),
            ("float", float_pipeline, IMPUTE_FLOAT_COLS),
        ],
        remainder="drop",
        verbose_feature_names_out=True,
    )
