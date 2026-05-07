import csv
import json
from pathlib import Path

from datahub.variant_viewer_canonicalization import (
    canonicalize_variant_viewer_artifacts,
    find_macos_hidden_files,
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _write_tree(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "CVD": {
                    "Cerebrovascular": ["stroke"],
                    "Hypertension": ["hypertension", "ocular hypertension"],
                    "Arrhythmia": ["sudden cardiac arrest"],
                },
                "TRAIT": {
                    "Lipid Profile": ["triglyceride levels"],
                },
            }
        )
    )


def test_find_macos_hidden_files(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    (raw_root / "cvd").mkdir(parents=True)
    (raw_root / "cvd" / "._stroke.txt").write_text("metadata")
    (raw_root / "cvd" / ".DS_Store").write_text("metadata")
    (raw_root / "cvd" / "stroke.txt").write_text("real")

    found = find_macos_hidden_files(raw_root)

    assert [path.name for path in found] == [".DS_Store", "._stroke.txt"]


def test_canonicalize_variant_viewer_artifacts_merges_aliases_and_drops_resource_fork_rows(
    tmp_path: Path,
) -> None:
    root = tmp_path / "variant_viewer"
    raw_root = tmp_path / "raw"
    tree_path = tmp_path / "phenotype_tree.json"
    _write_tree(tree_path)
    (raw_root / "cvd").mkdir(parents=True)
    (raw_root / "cvd" / "._stroke.txt").write_text("metadata")

    _write_csv(
        root / "phenotypes" / "stroke" / "GENE.csv",
        [{"gene_name": "GENE", "phenotype": "stroke", "amino_acid": "A/T"}],
    )
    _write_csv(
        root / "phenotypes" / "troke" / "GENE.csv",
        [{"gene_name": "GENE", "phenotype": "troke", "amino_acid": "G/V"}],
    )
    _write_csv(
        root / "phenotypes" / "hypertensi" / "GENE.csv",
        [{"gene_name": "GENE", "phenotype": "hypertensi", "amino_acid": "D/E"}],
    )
    _write_csv(
        root / "phenotypes" / "roke" / "roke_BAD.csv",
        [{"gene_name": "roke_BAD", "phenotype": "roke", "amino_acid": ""}],
    )
    _write_csv(
        root / "overall" / "GENE.csv",
        [
            {"gene_name": "GENE", "phenotype": "troke", "amino_acid": "G/V"},
            {"gene_name": "GENE", "phenotype": "hypertensi", "amino_acid": "D/E"},
        ],
    )
    _write_csv(
        root / "overall" / "roke_BAD.csv",
        [{"gene_name": "roke_BAD", "phenotype": "roke", "amino_acid": ""}],
    )

    summary = canonicalize_variant_viewer_artifacts(
        variant_viewer_root=root,
        phenotype_tree_json=tree_path,
        raw_root=raw_root,
        remove_hidden_raw=True,
        reset_checkpoint=True,
        fail_on_unknown=True,
        report_path=root / "report.json",
    )

    assert not (raw_root / "cvd" / "._stroke.txt").exists()
    assert not (root / "phenotypes" / "troke").exists()
    assert not (root / "phenotypes" / "roke").exists()
    assert not (root / "overall" / "roke_BAD.csv").exists()
    assert {row["phenotype"] for row in _read_csv(root / "phenotypes" / "stroke" / "GENE.csv")} == {
        "stroke"
    }
    assert {row["phenotype"] for row in _read_csv(root / "phenotypes" / "hypertension" / "GENE.csv")} == {
        "hypertension"
    }
    assert {row["phenotype"] for row in _read_csv(root / "overall" / "GENE.csv")} == {
        "stroke",
        "hypertension",
    }
    assert summary["hidden_raw_files_removed"] == 1
    assert summary["unknown_phenotype_dirs"] == []
    assert summary["unknown_row_labels"] == []
