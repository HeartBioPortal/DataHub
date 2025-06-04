#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from jsonschema import validate, ValidationError

SCHEMA_PATH = Path(__file__).resolve().parents[1] / 'schemas' / 'metadata.schema.json'


def validate_file(meta_file: Path) -> int:
    try:
        schema = json.load(open(SCHEMA_PATH))
        data = json.load(open(meta_file))
        validate(data, schema)
        return 0
    except (ValidationError, json.JSONDecodeError) as exc:
        print(f'Validation error for {meta_file}: {exc}')
        return 1


def main(paths):
    errors = 0
    for p in paths:
        meta_path = Path(p)
        if meta_path.is_dir():
            meta_path = meta_path / 'metadata.json'
        if meta_path.exists():
            errors += validate_file(meta_path)
        else:
            print(f'Metadata file not found: {meta_path}')
            errors += 1
    return errors

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: validate_metadata.py <path> [<path>...]')
        sys.exit(1)
    sys.exit(main(sys.argv[1:]))
