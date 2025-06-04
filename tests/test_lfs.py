from pathlib import Path

def test_lfs_attributes():
    attrs = Path('.gitattributes').read_text()
    assert 'filter=lfs' in attrs
