from __future__ import annotations
from typing import Tuple
import pandas as pd

def time_split(
    df: pd.DataFrame,
    *,
    target_col: str,
    train_frac: float,
    tune_frac: float,
    gap_steps: int,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    if "step" not in df.columns:
        raise ValueError("df must contain a 'step' column for time splitting.")
    if target_col not in df.columns:
        raise ValueError(f"target_col {target_col!r} not in df columns.")

    if not (0.0 < train_frac < tune_frac < 1.0):
        raise ValueError("Require 0 < train_frac < tune_frac < 1.")

    max_step = int(df["step"].max())
    train_end = int(max_step * train_frac)
    tune_end  = int(max_step * tune_frac)

    train_mask = df["step"] <= (train_end - gap_steps)
    tune_mask  = (df["step"] > train_end) & (df["step"] <= tune_end)
    hold_mask  = df["step"] > tune_end

    y = df[target_col]
    X = df.drop(columns=[target_col])

    return (
        X.loc[train_mask], y.loc[train_mask],
        X.loc[tune_mask],  y.loc[tune_mask],
        X.loc[hold_mask],  y.loc[hold_mask],
    )
