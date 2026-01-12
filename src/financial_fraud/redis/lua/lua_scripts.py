"""Atomic update lua scripts."""

from __future__ import annotations

from importlib import resources
from typing import Final

_LUA_PKG = "financial_fraud.redis.lua"

SCRIPT_DEST_ADVANCE: Final[str] = (
    resources.files(_LUA_PKG).joinpath("advance.lua").read_text(encoding="utf-8")
)

SCRIPT_DEST_ADD: Final[str] = (
    resources.files(_LUA_PKG).joinpath("add.lua").read_text(encoding="utf-8")
)
