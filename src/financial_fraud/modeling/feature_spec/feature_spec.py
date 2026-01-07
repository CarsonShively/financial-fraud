from __future__ import annotations

from typing import Any, Dict

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from financial_fraud.modeling.feature_spec.apply import feature_spec


class FeatureSpecTransformer(BaseEstimator, TransformerMixin):

    DROP_COLS = ["name_orig", "name_dest", "step"]

    def __init__(self, spec: Dict[str, Any]):
        self.spec = spec

    def fit(self, X: pd.DataFrame, y=None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            raise TypeError("FeatureSpecTransformer expects a pandas DataFrame as input.")

        df = X.copy()
        try:
            df = df.drop(columns=self.DROP_COLS)
        except KeyError as e:
            missing = [c for c in self.DROP_COLS if c not in df.columns]
            raise KeyError(
                f"FeatureSpecTransformer expected columns to drop {self.DROP_COLS}, "
                f"but missing={missing}. Available columns={list(df.columns)}"
            ) from e

        return feature_spec(df, self.spec)
