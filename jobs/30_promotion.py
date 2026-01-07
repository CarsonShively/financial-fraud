from __future__ import annotations

import argparse
import logging
from pathlib import Path

from financial_fraud.config import REPO_ID, REVISION
from financial_fraud.io.hf import read_model_json, upload_model_json_hf
from financial_fraud.io.hf_run_metrics import fetch_all_run_metrics
from financial_fraud.promotion.best_candidate import get_best_contender
from financial_fraud.promotion.decision import decide_promotion
from financial_fraud.promotion.registry import ChampionRef, write_champion_json
from financial_fraud.logging_utils import setup_logging

log = logging.getLogger(__name__)

CHAMPION_PATH = "champion.json"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHAMPION_PATH_LOCAL = PROJECT_ROOT / CHAMPION_PATH
EPSILON = 0.001

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select the best contender and optionally promote it to champion."
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="If set, apply the promotion (write/upload champion.json). Otherwise, dry-run only.",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    setup_logging()
    log = logging.getLogger(__name__)
    log.info("Starting promotion pipeline...")

    rows = fetch_all_run_metrics(repo_id=REPO_ID, revision=REVISION)
    
    rows = [r for r in rows if r.metadata.get("role") == "candidate"]

    try:
        best_row = get_best_contender(rows)
    except ValueError as e:
        log.error("Promotion pipeline stopped: %s", e)
        return

    champion_ptr = read_model_json(
        repo_id=REPO_ID,
        revision=REVISION,
        path_in_repo=CHAMPION_PATH,
    )
    
    if champion_ptr == None:
        champion_metrics = None
    else:
        champion_metrics = read_model_json(
            repo_id=REPO_ID,
            revision=REVISION,
            path_in_repo=f'{champion_ptr["path_in_repo"]}/metrics.json',
        )

    contender_metrics = best_row.metrics

    decision = decide_promotion(
        contender_metrics=contender_metrics,
        champion_metrics=champion_metrics,
        epsilon=EPSILON,
    )

    log.info(
        "Promotion decision: promote=%s primary=%s contender=%.6f champion=%s reason=%s",
        decision.promote,
        decision.primary_metric,
        decision.contender_primary,
        f"{decision.champion_primary:.6f}" if decision.champion_primary is not None else "None",
        decision.reason,
    )

    if not decision.promote:
        return

    run_id = best_row.run_id
    ref = ChampionRef(run_id=run_id, path_in_repo=f"runs/{run_id}")

    local_path = write_champion_json(ref, out_path=CHAMPION_PATH_LOCAL)
    log.info("Wrote local champion pointer: %s", local_path)

    if args.promote:
        upload_model_json_hf(
            local_path,
            repo_id=REPO_ID,
            path_in_repo=CHAMPION_PATH,
            revision=REVISION,
            commit_message=f"Update champion pointer -> {run_id}",
        )
        log.info("Uploaded champion.json to model repo: %s (rev=%s)", REPO_ID, REVISION)
    else:
        log.info("Dry-run (no upload). Pass --promote to apply.")
    return        

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Promotion pipeline failed")
        raise 