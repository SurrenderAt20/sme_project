import pandas as pd
from pipeline.transform import basic_clean, make_binary_target, prepare_features

def test_basic_clean_removes_duplicates():
    df = pd.DataFrame({'a': [1, 1], 'b': [2, 2]})
    cleaned = basic_clean(df)
    assert len(cleaned) == 1


def test_make_binary_target():
    df = pd.DataFrame({'deposit': ['yes', 'no', 'yes']})
    y = make_binary_target(df)
    assert list(y) == [1, 0, 1]


def test_prepare_features_shapes():
    df = pd.DataFrame({'deposit': ['yes', 'no'], 'x': [1, 2], 'y': ['a', 'b']})
    X, y = prepare_features(df)
    assert X.shape[0] == 2
    assert y.shape[0] == 2
