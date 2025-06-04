import subprocess
from pathlib import Path


def test_provenance_valid():
    ds = Path('public/example_fh_vcf')
    result = subprocess.run(['tools/hbp-validate', str(ds)], capture_output=True)
    assert result.returncode == 0, result.stdout.decode() + result.stderr.decode()
