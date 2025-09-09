import subprocess
import os

def test_pipeline_end_to_end():
    # Assumes data/bank.csv exists and main.py is runnable
    result = subprocess.run([
        'python', 'main.py', '--data', 'data/bank.csv'
    ], cwd=os.path.join(os.getcwd(), 'ai_compliance_pipeline'), capture_output=True)
    assert result.returncode == 0
    # Check that artifacts are created
    artifacts_dir = os.path.join(os.getcwd(), 'ai_compliance_pipeline', 'artifacts')
    assert os.path.exists(artifacts_dir)
