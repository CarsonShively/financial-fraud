"""
Create champion pointer ref.
"""

import json
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class ChampionRef:
    run_id: str
    path_in_repo: str

def write_champion_json(
    ref: ChampionRef,
    *,
    out_path: str | Path = "champion.json",
) -> Path:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    obj = {
        "run_id": ref.run_id,
        "path_in_repo": ref.path_in_repo,
    }

    p.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return p