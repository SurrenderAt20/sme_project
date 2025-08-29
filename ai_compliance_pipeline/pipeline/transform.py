import pandas as pd

# For your dataset, the target column is the yes/no column named "deposit"
TARGET_COL = "deposit"

def make_binary_target(df: pd.DataFrame, target_col: str = TARGET_COL) -> pd.Series:
    """
    Convert a yes/no target column into 1/0.
    yes -> 1, no -> 0
    """
    if target_col not in df.columns:
        raise ValueError(f"Expected target column '{target_col}' in dataset.")
    y = (df[target_col].astype(str).str.lower() == "yes").astype(int)
    return y