"""
Apply the feature spec as a model contract.
"""

from __future__ import annotations

from typing import Any, Dict, List
import pandas as pd

_DTYPE_MAP = {
    "string": "string",
    "int": "Int64",
    "float": "Float64",
    "bool": "boolean",
    "category": "category",
}

def feature_spec(df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    out = df.copy()

    req: List[Dict[str, Any]] = []
    req.extend(spec.get("features", []))

    ordered = [c["name"] for c in req]
    allowed = set(ordered)

    missing = [name for name in ordered if name not in out.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    extras = [col for col in out.columns if col not in allowed]
    if extras:
        raise KeyError(f"Unexpected extra columns (not in spec): {extras}")

    for c in req:
        name = c["name"]
        dtype = c.get("dtype")
        if not dtype:
            continue

        pandas_dtype = _DTYPE_MAP.get(dtype)
        if pandas_dtype is None:
            raise ValueError(f"Unknown dtype {dtype!r} for column {name!r}")

        if dtype == "int":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Int64")

        elif dtype == "float":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Float64")

        elif dtype == "category":
            allowed_cats = c.get("categories")
            if allowed_cats is None:
                raise ValueError(
                    f"Spec column {name!r} has dtype 'category' but no 'categories' list"
                )
            s = out[name].astype("string")
            s = s.where(s.isna() | s.isin(allowed_cats), pd.NA)
            out[name] = pd.Categorical(s, categories=allowed_cats)

        else:
            out[name] = out[name].astype(pandas_dtype)

    out = out[ordered]

    return out
