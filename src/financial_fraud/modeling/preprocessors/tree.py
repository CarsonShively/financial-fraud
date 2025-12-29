import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer
from sklearn.compose import ColumnTransformer


def preprocessor() -> ColumnTransformer:
    cat_ohe = ["type"]
    type_categories = [["payment", "transfer", "cash_out", "debit", "cash_in", "unknown"]]

    num_log1p_scale = [
        "amount",
        "orig_txn_count", "orig_amount_sum",
        "dest_txn_count", "dest_amount_sum",
        "orig_amount_mean", "dest_amount_mean",
    ]

    num_scale_only = [
        "step", "orig_last_step", "dest_last_step",
        "orig_balance_delta_sum", "dest_balance_delta_sum",
    ]

    ohe_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
            ("ohe", OneHotEncoder(categories=type_categories, handle_unknown="ignore", sparse_output=False)),
        ]
    )

    log1p_scaler_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ]
    )

    scaler_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("cat", ohe_pipeline, cat_ohe),
            ("num_log1p", log1p_scaler_pipeline, num_log1p_scale),
            ("num", scaler_pipeline, num_scale_only),
        ],
        remainder="drop",
        verbose_feature_names_out=True,
    )
