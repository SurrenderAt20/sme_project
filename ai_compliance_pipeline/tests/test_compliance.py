import os
import json
from pipeline.compliance import write_json

def test_metadata_file_creation():
    data = {'run_id': 'test', 'dataset': {'rows': 2}}
    path = 'test_metadata.json'
    write_json(path, data)
    assert os.path.exists(path)
    with open(path) as f:
        loaded = json.load(f)
    assert loaded['run_id'] == 'test'
    os.remove(path)
