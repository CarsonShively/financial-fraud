"""
Create local log for demo transactions.
"""

import pandas as pd

def local_log(rows: list[dict], one_row_df: pd.DataFrame, *, max_len: int = 200) -> list[dict]:
    if one_row_df is None or one_row_df.empty:
        return rows
    rows.append(one_row_df.iloc[0].to_dict())
    if len(rows) > max_len:
        rows = rows[-max_len:]
    return rows