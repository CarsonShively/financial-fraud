from __future__ import annotations

from dataclasses import dataclass, field

import optuna
from lightgbm import LGBMClassifier
from sklearn.pipeline import Pipeline

from financial_fraud.modeling.feature_spec.feature_spec import FeatureSpecTransformer
from financial_fraud.modeling.feature_spec.load import load_feature_spec
from financial_fraud.modeling.preprocessors.tree import preprocessor  


@dataclass(slots=True)
class LGBTrainer:
    seed: int = 42
    spec: dict = field(default_factory=load_feature_spec)

    def build_pipeline(self) -> Pipeline:
        return Pipeline(
            steps=[
                ("spec", FeatureSpecTransformer(self.spec)),
                ("pre", preprocessor()),
                ("clf", LGBMClassifier(
                    random_state=self.seed,
                    n_jobs=-1,
                    objective="binary",
                    verbose=-1,

                    n_estimators=600,
                    learning_rate=0.05,

                    boosting_type="gbdt",
                    max_bin=255,
                    num_leaves=63,
                    max_depth=-1,
                    min_child_samples=100,

                    subsample=0.8,
                    subsample_freq=1,
                    colsample_bytree=0.8,

                    reg_lambda=10.0,
                ))

            ]
        )

