#!/usr/bin/env python3
"""List available dataset IDs and titles."""
from __future__ import annotations
import json
from pathlib import Path

def iterate_datasets(base: Path):
    for meta_path in base.glob('*/metadata.json'):
        with meta_path.open() as fh:
            data = json.load(fh)
        yield data.get('dataset_id', meta_path.parent.name), data.get('title', '')

def main() -> int:
    roots = [Path('public'), Path('private')]
    for root in roots:
        if not root.exists():
            continue
        for ds_id, title in iterate_datasets(root):
            print(f"{ds_id}\t{title}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
