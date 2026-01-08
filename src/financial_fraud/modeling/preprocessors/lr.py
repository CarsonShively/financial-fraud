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
        "dest_amount_sum_1h",
        "dest_amount_sum_24h",
        "dest_amount_mean_24h",
    ]

    num_scale_only = [
        "orig_balance_delta",
        "orig_delta_minus_amount",
        "dest_balance_delta",
        "dest_delta_minus_amount",
        "dest_txn_count_1h",
        "dest_txn_count_24h",
        "dest_last_gap_hours",
    ]

    bin_cols = [
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

    log1p_scaler_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
            ("log1p", FunctionTransformer(np.log1p, feature_names_out="one-to-one")),
            ("scaler", StandardScaler()),
        ]
    )

    scaler_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
            ("scaler", StandardScaler()),
        ]
    )

    bin_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("cat", ohe_pipeline, cat_ohe),
            ("num_log1p", log1p_scaler_pipeline, num_log1p_scale),
            ("num", scaler_pipeline, num_scale_only),
            ("bin", bin_pipeline, bin_cols),
        ],
        remainder="drop",
        verbose_feature_names_out=True,
    )
