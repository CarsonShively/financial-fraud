from __future__ import annotations

from dataclasses import dataclass, field
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier

from financial_fraud.modeling.feature_spec.feature_spec import FeatureSpecTransformer
from financial_fraud.modeling.feature_spec.load import load_feature_spec
from financial_fraud.modeling.preprocessors.tree import preprocessor


@dataclass(slots=True)
class XGBTrainer:
    seed: int = 42
    spec: dict = field(default_factory=load_feature_spec)

    def build_pipeline(self) -> Pipeline:
        return Pipeline(
            steps=[
                ("spec", FeatureSpecTransformer(self.spec)),
                ("pre", preprocessor()),
                ("clf", XGBClassifier(
                    random_state=self.seed,
                    n_jobs=-1,

                    objective="binary:logistic",
                    eval_metric="logloss",
                    tree_method="hist",
                    max_bin=256,
                    verbosity=0,

                    n_estimators=600,
                    learning_rate=0.05,

                    max_depth=4,
                    min_child_weight=10.0,

                    subsample=0.8,
                    colsample_bytree=0.8,

                    reg_lambda=10.0,
                    gamma=0.0,

                    reg_alpha=0.0,
                )),
            ]
        )
