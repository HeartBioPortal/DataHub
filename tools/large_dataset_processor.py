#!/usr/bin/env python3
"""Utility for analyzing large VCF datasets using Dask.

This script demonstrates how to process variant call format (VCF) files that may
exceed hundreds of gigabytes in size. It uses Dask's lazy computation model to
enable out-of-core processing so that the data can be analysed on machines with
limited memory.
"""
import argparse
import dask.bag as db
from typing import Union


def count_variants(vcf_path: Union[str, list[str]]) -> int:
    """Count variant records in a VCF file.

    Parameters
    ----------
    vcf_path: str or list
        Path(s) to VCF file(s). Wildcards are allowed to process sharded data.
    Returns
    -------
    int
        Number of variant records found.
    """
    lines = db.read_text(vcf_path)
    variant_lines = lines.filter(lambda x: not x.startswith('#'))
    return variant_lines.count().compute()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Process large VCF datasets using Dask")
    parser.add_argument("vcf", help="Path to the VCF file or pattern")
    args = parser.parse_args(argv)

    count = count_variants(args.vcf)
    print(f"Total variant records: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
