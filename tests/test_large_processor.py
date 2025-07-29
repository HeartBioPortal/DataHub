import subprocess
from pathlib import Path


def test_variant_count():
    vcf = Path('public/example_fh_vcf/variant.vcf')
    result = subprocess.run(
        ['tools/large_dataset_processor.py', str(vcf)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert 'Total variant records: 1' in result.stdout
