from __future__ import annotations
from pathlib import Path
from huggingface_hub import hf_hub_download, HfApi
from typing import Optional, Any
import json
from huggingface_hub.utils import EntryNotFoundError
import joblib

def download_dataset_hf(repo_id: str, filename: str, revision: str = "main") -> str:
    """Download a single file from a Hugging Face dataset repo using the normal HF cache."""
    return hf_hub_download(
        repo_id=repo_id,
        filename=filename,
        repo_type="dataset",
        revision=revision,
    )

def upload_dataset_hf(
    *,
    local_path: str | Path,
    repo_id: str,
    hf_path: str,
    revision: str = "main",
    commit_message: str | None = None,
) -> None:
    """Upload one local file to a Hugging Face dataset repo at hf_path."""
    p = Path(local_path)
    if not p.exists():
        raise FileNotFoundError(f"Local path not found: {p}")
    if not p.is_file():
        raise IsADirectoryError(f"Expected a file, got: {p}")

    api = HfApi()
    api.upload_file(
        path_or_fileobj=str(p),
        path_in_repo=hf_path,
        repo_id=repo_id,
        repo_type="dataset",
        revision=revision,
        commit_message=commit_message or f"Upload {hf_path}",
    )
    
def upload_model_bundle(
    bundle_dir: str | Path,
    *,
    repo_id: str,
    run_id: str,
    revision: str = "main",
    commit_message: Optional[str] = None,
    ensure_new: bool = True,
) -> str:
    """Upload a local run bundle directory to a HF model repo under runs/{run_id}."""
    p = Path(bundle_dir)

    if not p.exists() or not p.is_dir():
        raise FileNotFoundError(f"Bundle directory not found: {p}")

    inferred = p.name
    if run_id != inferred:
        raise ValueError(f"run_id mismatch: arg={run_id} folder={inferred}")

    path_in_repo = f"runs/{run_id}"
    api = HfApi()

    if ensure_new:
        existing_files = api.list_repo_files(
            repo_id=repo_id,
            repo_type="model",
            revision=revision,
        )
        prefix = path_in_repo + "/"
        if any(f.startswith(prefix) for f in existing_files):
            raise FileExistsError(
                f"Remote run folder already exists: {path_in_repo} "
                f"(choose a new run_id or disable ensure_new)"
            )

    api.upload_folder(
        folder_path=str(p),
        repo_id=repo_id,
        repo_type="model",
        revision=revision,
        path_in_repo=path_in_repo,
        commit_message=commit_message or f"Upload run {run_id}",
    )

    return path_in_repo

def read_model_json(
    *,
    repo_id: str,
    revision: str,
    path_in_repo: str,
) -> Optional[dict[str, Any]]:
    """Download and parse a JSON file from a Hugging Face model repo (returns None if missing)."""
    try:
        local_file = hf_hub_download(
            repo_id=repo_id,
            repo_type="model",
            revision=revision,
            filename=path_in_repo,
        )
    except EntryNotFoundError:
        return None

    with open(local_file, "r", encoding="utf-8") as f:
        return json.load(f)
    
def upload_model_json_hf(
    local_path: str | Path,
    *,
    repo_id: str,
    path_in_repo: str | None = None,
    revision: str = "main",
    commit_message: str | None = None,
) -> None:
    """Upload a local JSON file to a Hugging Face model repo at the given repo path."""
    p = Path(local_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Local file not found: {p}")

    dest = path_in_repo or p.name
    msg = commit_message or f"Upload {dest}"

    api = HfApi()
    api.upload_file(
        path_or_fileobj=str(p),
        path_in_repo=dest,
        repo_id=repo_id,
        repo_type="model",
        revision=revision,
        commit_message=msg,
    )
    
def load_model_hf(*, repo_id: str, revision: str, path_in_repo: str) -> Any:
    """Download a model artifact from HF and load it with joblib."""
    local_file = hf_hub_download(
        repo_id=repo_id,
        repo_type="model",
        revision=revision,
        filename=path_in_repo,
    )
    return joblib.load(local_file)