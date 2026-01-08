from __future__ import annotations
import pandas as pd


def time_split(
    df: pd.DataFrame,
    *,
    target_col: str,
    train_frac: float,
    tune_frac: float,
    gap_steps: int = 0,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    if "step" not in df.columns:
        raise ValueError("df must contain a 'step' column.")
    if target_col not in df.columns:
        raise ValueError(f"target_col {target_col!r} not in df columns.")
    if not (0.0 < train_frac < tune_frac < 1.0):
        raise ValueError("Require 0 < train_frac < tune_frac < 1.")
    if gap_steps < 0:
        raise ValueError("gap_steps must be >= 0.")

    df = df.sort_values("step").reset_index(drop=True)

    max_step = int(df["step"].max())
    train_end = int(max_step * train_frac)
    tune_end = int(max_step * tune_frac)

    s = df["step"]
    train = s <= train_end
    tune  = (s > train_end + gap_steps) & (s <= tune_end)
    hold  = s > tune_end + gap_steps

    y = df[target_col]
    X = df.drop(columns=[target_col, "txn_id"])

    return X[train], y[train], X[tune], y[tune], X[hold], y[hold]
