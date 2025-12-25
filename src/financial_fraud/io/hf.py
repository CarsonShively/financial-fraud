from __future__ import annotations

from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download

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