import argparse
from pathlib import Path
from time import perf_counter
import pandas as pd

import numpy as np

from financial_fraud.modeling.run_id import make_run_id
from financial_fraud.modeling.metrics.report import project_metric_report
from financial_fraud.io.hf import download_dataset_hf, upload_model_bundle
from financial_fraud.modeling.bundle.write_bundle import write_bundle
from financial_fraud.modeling.splits import time_split
from financial_fraud.modeling.fit import fit_pipeline
from financial_fraud.modeling.threshold import tune_threshold
from financial_fraud.modeling.evaluate import evaluate
from financial_fraud.modeling.gate_broken import gate_broken
from financial_fraud.modeling.trainers.make_trainer import make_trainer, available_trainers
from financial_fraud.modeling.bundle.model_artifact import ModelArtifact

from financial_fraud.config import (
    REPO_ID,
    REVISION,
    TRAIN_HF_PATH,
    CURRENT_ARTIFACT_VERSION,
)

from financial_fraud.modeling.config import (
    TARGET_COL,
    PRIMARY_METRIC,
    METRIC_DIRECTION,
    GAP_STEPS,
    TUNE_END_FRAC,
    TRAIN_END_FRAC,
    SEED,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_RUNS_DIR = REPO_ROOT / "artifacts" / "runs"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--model-type",
        dest="model_type",
        required=True,
        choices=available_trainers(),
        help="Which trainer/model family to use",
    )
    p.add_argument(
        "--role",
        choices=["candidate", "baseline"],
        default="candidate",
        help="Run role. Baseline runs are not promotable.",
    )
    p.add_argument("--upload", action="store_true")
    return p.parse_args()


def main(*, modeltype: str, role: str, upload: bool = False) -> None:
    t0 = perf_counter()
    run_id = make_run_id()

    metrics = project_metric_report()

    trainer = make_trainer(modeltype, seed=SEED)

    local_path = download_dataset_hf(
        repo_id=REPO_ID, filename=TRAIN_HF_PATH, revision=REVISION
    )

    df = pd.read_parquet(local_path)

    X_train, y_train, X_tune, y_tune, X_hold, y_hold = time_split(
        df,
        target_col=TARGET_COL,
        train_frac=TRAIN_END_FRAC,
        tune_frac=TUNE_END_FRAC,
        gap_steps=GAP_STEPS,
    )

    artifact, feature_names = fit_pipeline(
        build_pipeline=trainer.build_pipeline,
        X=X_train,
        y=y_train,
    )

    y_score_tune = artifact.predict_proba(X_tune)[:, 1]

    tuned_threshold = tune_threshold(
        y_score=np.asarray(y_score_tune),
        flag_rate=0.05,  
    )

    holdout_metrics = evaluate(
        artifact,
        X_hold,
        y_hold,
        metrics=metrics,
        threshold=tuned_threshold,
    )

    y_score_train = artifact.predict_proba(X_train)[:, 1]
    y_score_hold = artifact.predict_proba(X_hold)[:, 1]

    gate = gate_broken(
        y_true_hold=y_hold,
        y_score_hold=y_score_hold,
        y_true_train=y_train,
        y_score_train=y_score_train,
    )


    if not gate["ok"]:
        raise ValueError(f"Upload gated: {gate}")


    artifact_obj = ModelArtifact(
        run_id=run_id,
        artifact_version=CURRENT_ARTIFACT_VERSION,
        model_type=modeltype,
        model=artifact,
        role=role,
        threshold=tuned_threshold
    )

    bundle_dir = ARTIFACT_RUNS_DIR / run_id
    bundle_dir.mkdir(parents=True, exist_ok=True)

    cfg = {
        "repo_id": REPO_ID,
        "revision": REVISION,
        "train_hf_path": TRAIN_HF_PATH,
        "target_col": TARGET_COL,
        "primary_metric": PRIMARY_METRIC,
        "direction": METRIC_DIRECTION,
        "threshold": tuned_threshold,
        "seed": SEED,
    }

    write_bundle(
        bundle_dir=bundle_dir,
        artifact_version=CURRENT_ARTIFACT_VERSION,
        artifact_obj=artifact_obj,
        holdout_metrics=holdout_metrics,
        primary_metric=PRIMARY_METRIC,
        direction=METRIC_DIRECTION,
        threshold=tuned_threshold,
        feature_names=feature_names,
        cfg=cfg,
    )

    required = ["model.joblib", "metrics.json", "metadata.json"]
    missing = [name for name in required if not (bundle_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Bundle missing required files: {missing} in {bundle_dir}")

    if upload:
        upload_model_bundle(
            bundle_dir=bundle_dir, repo_id=REPO_ID, run_id=run_id, revision=REVISION
        )

    _elapsed = perf_counter() - t0

if __name__ == "__main__":
    args = parse_args()
    main(modeltype=args.model_type, role=args.role, upload=args.upload)
