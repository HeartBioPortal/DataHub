#!/usr/bin/env python3
"""
Validate one or more metadata JSON files against a JSON Schema.

Key features:
- Recurses into directories (optional) and supports a filename pattern.
- Reports ALL schema errors per file (not just the first).
- Shows JSON pointer-like paths to failing fields.
- Can emit machine-readable JSON for CI.
- Parallel validation for speed on large trees.
- Proper exit codes: 0=ok, 1=errors, 2=no files found.
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import os
import sys
from pathlib import Path
from typing import Iterable, List, Dict, Any

from jsonschema import exceptions as jsex
from jsonschema.validators import validator_for
from jsonschema import FormatChecker


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "metadata.schema.json"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def compile_validator(schema_path: Path):
    schema = load_json(schema_path)
    # Pick the correct validator for the schema's declared draft.
    Validator = validator_for(schema)
    Validator.check_schema(schema)
    base_uri = schema_path.parent.as_uri().rstrip("/") + "/"
    # Use a generic format checker (supports common formats like date-time, email, uri)
    return Validator(schema, format_checker=FormatChecker()), base_uri


def find_metadata_files(inputs: Iterable[str], pattern: str, recursive: bool) -> List[Path]:
    results: List[Path] = []
    for raw in inputs:
        p = Path(raw)
        if p.is_dir():
            if recursive:
                results.extend(sorted(p.rglob(pattern)))
            else:
                candidate = p / pattern
                if candidate.exists():
                    results.append(candidate)
        else:
            # Treat direct files (can be any filename)
            if p.exists():
                results.append(p)
    return results


def error_to_dict(err: jsex.ValidationError, file: Path) -> Dict[str, Any]:
    return {
        "file": str(file),
        "type": "schema",
        "message": err.message,
        "path": "/" + "/".join(str(x) for x in err.path),            # where in the instance
        "schema_path": "/" + "/".join(str(x) for x in err.schema_path),
        "validator": err.validator,
        "validator_value": err.validator_value,
    }


def decode_error_to_dict(exc: json.JSONDecodeError, file: Path) -> Dict[str, Any]:
    return {
        "file": str(file),
        "type": "json",
        "message": exc.msg,
        "line": exc.lineno,
        "column": exc.colno,
    }


def os_error_to_dict(exc: OSError, file: Path) -> Dict[str, Any]:
    return {
        "file": str(file),
        "type": "io",
        "message": f"{exc.__class__.__name__}: {exc}",
    }


def validate_one(file: Path, validator) -> Dict[str, Any]:
    """Return a result dict with 'file', 'ok', and 'errors' (list)."""
    result: Dict[str, Any] = {"file": str(file), "ok": True, "errors": []}
    try:
        data = load_json(file)
    except json.JSONDecodeError as e:
        result["ok"] = False
        result["errors"].append(decode_error_to_dict(e, file))
        return result
    except OSError as e:
        result["ok"] = False
        result["errors"].append(os_error_to_dict(e, file))
        return result

    errors = list(validator.iter_errors(data))
    if errors:
        result["ok"] = False
        result["errors"] = [error_to_dict(e, file) for e in sorted(errors, key=lambda x: x.path)]
    return result


def print_text_result(res: Dict[str, Any], show_ok: bool) -> None:
    if res["ok"]:
        if show_ok:
            print(f"OK: {res['file']}")
        return
    # Print each error as its own line.
    for e in res["errors"]:
        if e["type"] == "schema":
            print(
                f"ERROR: {e['file']} :: {e['message']} "
                f"(path={e['path']} schema={e['schema_path']} validator={e['validator']})"
            )
        elif e["type"] == "json":
            print(
                f"ERROR: {e['file']} :: JSON parse error: {e['message']} "
                f"(line {e['line']}, col {e['column']})"
            )
        else:
            print(f"ERROR: {e['file']} :: {e['message']}")


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Validate metadata JSON against a schema.")
    ap.add_argument("paths", nargs="+", help="Files or directories to validate.")
    ap.add_argument("-s", "--schema", type=Path, default=DEFAULT_SCHEMA_PATH, help="Path to JSON Schema.")
    ap.add_argument("-p", "--pattern", default="metadata.json", help="Filename to look for in directories (default: metadata.json).")
    ap.add_argument("-r", "--recursive", action="store_true", help="Recurse into directories.")
    ap.add_argument("-j", "--jobs", type=int, default=max(os.cpu_count() or 2, 2), help="Parallel workers (default: CPU count).")
    ap.add_argument("--format", choices=["text", "json"], default="text", help="Output format.")
    ap.add_argument("--show-ok", action="store_true", help="Also print files that validated successfully.")
    args = ap.parse_args(argv)

    try:
        validator, _ = compile_validator(args.schema.resolve())
    except Exception as e:
        print(f"Failed to load/compile schema '{args.schema}': {e}", file=sys.stderr)
        return 1

    files = find_metadata_files(args.paths, args.pattern, args.recursive)
    if not files:
        print("No metadata files found.", file=sys.stderr)
        return 2

    results: List[Dict[str, Any]] = []
    with futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        for res in ex.map(lambda f: validate_one(f, validator), files):
            results.append(res)

    failed = sum(1 for r in results if not r["ok"])

    if args.format == "json":
        print(json.dumps({"summary": {"total": len(results), "failed": failed}, "results": results}, indent=2))
    else:
        for r in results:
            print_text_result(r, show_ok=args.show_ok)
        print(f"Summary: {len(results)} files checked, {failed} failed.", file=sys.stderr)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
