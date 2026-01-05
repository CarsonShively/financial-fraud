from __future__ import annotations

from importlib import resources
from typing import Final


SCRIPT_DEST_UPDATE: Final[str] = resources.files("financial_fraud.redis").joinpath(
    "update.lua"
).read_text(encoding="utf-8")