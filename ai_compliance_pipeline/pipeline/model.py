from pathlib import Path
from typing import Dict, Any
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
import joblib

def train_logreg(X_train, y_train, max_iter: int = 1000) -> LogisticRegression:
    m = LogisticRegression(max_iter=max_iter)
    m.fit(X_train, y_train)
    return m

def evaluate(model, X_test, y_test) -> Dict[str, Any]:
    preds = model.predict(X_test)
    return {"metric": "accuracy", "value": float(accuracy_score(y_test, preds))}

def save_model(model, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    return str(path)
