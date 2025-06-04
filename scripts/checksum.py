#!/usr/bin/env python3
import hashlib
import sys
from pathlib import Path


def md5sum(path: Path) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def main(filepath: str, expected: str) -> int:
    calc = md5sum(Path(filepath))
    if calc.lower() == expected.lower():
        return 0
    else:
        print(f'MD5 mismatch: {calc} != {expected}')
        return 1

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: checksum.py <file> <expected_md5>')
        sys.exit(1)
    sys.exit(main(sys.argv[1], sys.argv[2]))
