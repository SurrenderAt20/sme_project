import pandas as pd
from pipeline.model import train_logreg, evaluate, save_model
import os

def test_train_logreg_returns_model():
    X = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    y = pd.Series([0, 1])
    model = train_logreg(X, y)
    assert hasattr(model, 'predict')


def test_evaluate_accuracy():
    X = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    y = pd.Series([0, 1])
    model = train_logreg(X, y)
    metrics = evaluate(model, X, y)
    assert 'metric' in metrics and 'value' in metrics
    assert 0.0 <= metrics['value'] <= 1.0


def test_save_model_creates_file():
    X = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    y = pd.Series([0, 1])
    model = train_logreg(X, y)
    path = 'test_model.joblib'
    save_model(model, path)
    assert os.path.exists(path)
    os.remove(path)
