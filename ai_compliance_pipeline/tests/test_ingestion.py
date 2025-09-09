import pandas as pd
from pipeline.ingestion import load_csv, dataframe_schema
from pipeline.compliance import sha256_of_file

def test_load_csv_shape():
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    df.to_csv('test.csv', index=False)
    loaded = load_csv('test.csv')
    assert loaded.shape == (2, 2)


def test_dataframe_schema():
    df = pd.DataFrame({'a': [1], 'b': ['x']})
    schema = dataframe_schema(df)
    assert schema == {'a': 'int64', 'b': 'object'}


def test_sha256_of_file_changes():
    with open('test.txt', 'w') as f:
        f.write('abc')
    hash1 = sha256_of_file('test.txt')
    with open('test.txt', 'w') as f:
        f.write('abcd')
    hash2 = sha256_of_file('test.txt')
    assert hash1 != hash2
