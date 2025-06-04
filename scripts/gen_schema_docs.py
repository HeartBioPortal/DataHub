#!/usr/bin/env python3
import json
from pathlib import Path
from jsonschema2md import Parser

SCHEMA_DIR = Path(__file__).resolve().parents[1] / 'schemas'
DOCS_DIR = Path(__file__).resolve().parents[1] / 'docs'

parser = Parser()

for schema_file in ['metadata.schema.json', 'prov.schema.json']:
    schema_path = SCHEMA_DIR / schema_file
    if not schema_path.exists():
        continue
    schema = json.load(open(schema_path))
    lines = parser.parse_schema(schema)
    (DOCS_DIR / f'{schema_file}.md').write_text('\n'.join(lines))
