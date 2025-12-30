import argparse
import logging
from pathlib import Path
from time import perf_counter
import pandas as pd

from sklearn.metrics import average_precision_score
import numpy as np

from sklearn.model_selection import train_test_split
from financial_fraud.modeling.run_id import make_run_id
from financial_fraud.modeling.metrics.report import project_metric_report
from financial_fraud.io.hf import download_dataset_hf, upload_model_bundle
from financial_fraud.modeling.bundle.write_bundle import write_bundle
from financial_fraud.modeling.fit import fit_pipeline
from financial_fraud.modeling.evaluate import evaluate
from financial_fraud.modeling.trainers.make_trainer import make_trainer, available_trainers
from financial_fraud.modeling.bundle.model_artifact import ModelArtifact
from financial_fraud.logging_utils import setup_logging

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
    HOLDOUT_SIZE,
    SEED,
    THRESHOLD
)

log = logging.getLogger(__name__)

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
    p.add_argument("--upload", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()

def main(*, modeltype: str, upload: bool = False) -> None:
    """Train the specified model type and optionally upload the resulting run bundle."""
    t0 = perf_counter()
    run_id = make_run_id()

    log.info(
        "train start run_id=%s model_type=%s upload=%s repo=%s revision=%s train_path=%s",
        run_id, modeltype, upload, REPO_ID, REVISION, TRAIN_HF_PATH,
    )

    metrics = project_metric_report()
    if PRIMARY_METRIC not in metrics:
        raise ValueError(
            f"Unknown primary_metric {PRIMARY_METRIC!r}. Options: {sorted(metrics.keys())}"
        )
    primary_metric_fn = metrics[PRIMARY_METRIC]

    trainer = make_trainer(modeltype, seed=SEED)
    log.info("trainer ready: %s", modeltype)

    log.info("downloading train parquet from HF: repo=%s revision=%s file=%s", REPO_ID, REVISION, TRAIN_HF_PATH)
    local_path = download_dataset_hf(repo_id=REPO_ID, filename=TRAIN_HF_PATH, revision=REVISION)
    log.info("downloaded train parquet: %s", local_path)

    df = pd.read_parquet(local_path)
    log.info("loaded dataframe rows=%s cols=%s", len(df), len(df.columns))

    if TARGET_COL not in df.columns:
        raise ValueError(f"Target column '{TARGET_COL}' not found. Columns: {list(df.columns)}")

    y = df[TARGET_COL]
    X = df.drop(columns=[TARGET_COL])

    max_step = int(df["step"].max())
    cutoff = int(max_step * (1.0 - HOLDOUT_SIZE)) 

    GAP_STEPS = 24
    train_mask = df["step"] <= (cutoff - GAP_STEPS)
    hold_mask  = df["step"] > cutoff
    
    log.info(
        "step ranges: train=[%s,%s] holdout=[%s,%s] gap_steps=%s",
        int(df.loc[train_mask, "step"].min()),
        int(df.loc[train_mask, "step"].max()),
        int(df.loc[hold_mask, "step"].min()),
        int(df.loc[hold_mask, "step"].max()),
        GAP_STEPS,
    )
    log.info("split overlap_steps=%s", bool(
        set(df.loc[train_mask, "step"].unique()).intersection(set(df.loc[hold_mask, "step"].unique()))
    ))


    X_train = X.loc[train_mask]
    y_train = y.loc[train_mask]
    X_holdout = X.loc[hold_mask]
    y_holdout = y.loc[hold_mask]

    log.info(
        "time split by step cutoff=%s max_step=%s train_n=%s holdout_n=%s holdout_frac=%.3f",
        cutoff, max_step, len(X_train), len(X_holdout), len(X_holdout) / len(df),
    )
    log.info("fraud prevalence train=%.6f holdout=%.6f", float(y_train.mean()), float(y_holdout.mean()))

    log.info("fit best model")
    artifact, feature_names = fit_pipeline(
            build_pipeline=trainer.build_pipeline,
            X=X_train,
            y=y_train,
        )

    
    log.info("evaluate holdout threshold=%.3f", THRESHOLD)
    holdout_metrics = evaluate(
        artifact,
        X_holdout,
        y_holdout,
        metrics=metrics,
        threshold=THRESHOLD,
    )
    log.info("holdout metrics keys=%s", sorted(holdout_metrics.keys()))
    if PRIMARY_METRIC in holdout_metrics:
        log.info("holdout %s=%.6f", PRIMARY_METRIC, float(holdout_metrics[PRIMARY_METRIC]))

    if hasattr(artifact, "predict_proba") or hasattr(artifact, "decision_function"):
        y_score = artifact.predict_proba(X_holdout)[:, 1] if hasattr(artifact, "predict_proba") else artifact.decision_function(X_holdout)
        ap_real = average_precision_score(y_holdout, y_score)
        ap_shuf = average_precision_score(np.random.permutation(np.asarray(y_holdout)), y_score)
        prev = float(np.mean(y_holdout))
        log.info("sanity: prevalence=%.6f ap=%.6f ap_shuffled=%.6f", prev, ap_real, ap_shuf)

    
    artifact_obj = ModelArtifact(
        run_id=run_id,
        artifact_version=CURRENT_ARTIFACT_VERSION,
        model_type=modeltype,
        model=artifact,
        threshold=THRESHOLD,
    )

    bundle_dir = ARTIFACT_RUNS_DIR / run_id
    bundle_dir.mkdir(parents=True, exist_ok=True)
    log.info("writing bundle: %s", bundle_dir)

    cfg = {
        "repo_id": REPO_ID,
        "revision": REVISION,
        "train_hf_path": TRAIN_HF_PATH,
        "target_col": TARGET_COL,
        "primary_metric": PRIMARY_METRIC,
        "direction": METRIC_DIRECTION,
        "threshold": THRESHOLD,
        "seed": SEED,
        "holdout_size": HOLDOUT_SIZE,
    }

    write_bundle(
        bundle_dir=bundle_dir,
        artifact_version=CURRENT_ARTIFACT_VERSION,
        artifact_obj=artifact_obj,
        holdout_metrics=holdout_metrics,
        primary_metric=PRIMARY_METRIC,
        direction=METRIC_DIRECTION,
        feature_names=feature_names,
        cfg=cfg,
    )

    required = ["model.joblib", "metrics.json", "metadata.json"]
    missing = [name for name in required if not (bundle_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Bundle missing required files: {missing} in {bundle_dir}")
    log.info("bundle complete files_ok=%s", required)

    if upload:
        log.info("upload bundle start: repo=%s revision=%s run_id=%s", REPO_ID, REVISION, run_id)
        upload_model_bundle(bundle_dir=bundle_dir, repo_id=REPO_ID, run_id=run_id, revision=REVISION)
        log.info("upload bundle done")

    log.info("train done run_id=%s in %.2fs", run_id, perf_counter() - t0)

if __name__ == "__main__":
    args = parse_args()
    setup_logging(args.log_level)
    try:
        main(modeltype=args.model_type, upload=args.upload)
    except Exception:
        log.exception("train failed")
        raise