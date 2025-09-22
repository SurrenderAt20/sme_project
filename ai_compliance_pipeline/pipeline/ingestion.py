from pathlib import Path
import pandas as pd

def load_csv(csv_path: Path) -> pd.DataFrame:
    """Load a CSV file into a pandas DataFrame."""
    return pd.read_csv(csv_path)

def dataframe_schema(df: pd.DataFrame) -> dict:
    """Return a simple mapping of column dtype as strings."""
    return {col: str(dtype) for col, dtype in df.dtypes.items()}