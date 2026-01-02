from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class Reject:
    reason: str
    missing: tuple[str, ...] = ()
    null_fields: tuple[str, ...] = ()


def gate_tx(
    row: Mapping[str, Any],
    *,
    required: Iterable[str] = ("name_orig", "name_dest"),
) -> tuple[bool, Reject | None]:
    """
    Non-crashing gate.

    - Returns (True, None) if tx can proceed.
    - Returns (False, Reject(...)) if tx should be skipped.

    Designed to run AFTER silver_base_row(...).
    """
    req = tuple(required)

    missing_keys: list[str] = [k for k in req if k not in row]
    if missing_keys:
        return False, Reject(
            reason="missing_required_keys",
            missing=tuple(missing_keys),
        )

    null_fields: list[str] = [k for k in req if row.get(k) is None]
    if null_fields:
        return False, Reject(
            reason="required_fields_null",
            null_fields=tuple(null_fields),
        )

    return True, None
