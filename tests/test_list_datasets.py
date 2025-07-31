import subprocess

def test_list_includes_example():
    result = subprocess.run(['tools/list_datasets.py'], capture_output=True, text=True)
    assert result.returncode == 0
    assert 'example_fh_vcf' in result.stdout

