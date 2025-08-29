import pandas as pd
from sklearn.model_selection import train_test_split

TARGET_COL = "deposit"

def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates().reset_index(drop=True)

def make_binary_target(df: pd.DataFrame, target_col: str = TARGET_COL) -> pd.Series:
    if target_col not in df.columns:
        raise ValueError(f"Expected target column '{target_col}' in dataset.")
    return (df[target_col].astype(str).str.lower() == "yes").astype(int)

def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    y = make_binary_target(df, TARGET_COL)
    X = df.drop(columns=[TARGET_COL])
    X = pd.get_dummies(X, drop_first=True).fillna(0)
    return X, y

def train_test_split_simple(X, y, test_size: float = 0.2, random_state: int = 42):
    return train_test_split(X, y, test_size=test_size, random_state=random_state, stratify=y)
