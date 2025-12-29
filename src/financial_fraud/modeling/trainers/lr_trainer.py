from __future__ import annotations

from dataclasses import dataclass, field

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from financial_fraud.modeling.feature_spec.feature_spec import FeatureSpecTransformer
from financial_fraud.modeling.feature_spec.load import load_feature_spec
from financial_fraud.modeling.preprocessors.lr import preprocessor


@dataclass(slots=True)
class LRTrainer:
    """Log Regression trainer implementation for the telco-churn modeling pipeline."""
    seed: int = 42
    spec: dict = field(default_factory=load_feature_spec)

    def build_pipeline(self) -> Pipeline:
        return Pipeline(
            steps=[
                ("spec", FeatureSpecTransformer(self.spec)),
                ("pre", preprocessor()),
                ("clf", LogisticRegression(
                    solver="lbfgs",
                    penalty="l2",
                    C=1.0,
                    max_iter=2000,
                    tol=1e-4,
                    random_state=self.seed,
                ))

            ]
        )