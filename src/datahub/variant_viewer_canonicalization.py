"""Canonicalize legacy Variant Viewer artifacts against the phenotype tree."""

from __future__ import annotations

import csv
import json
import logging
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from datahub.adapters.phenotypes import PhenotypeMapper
from datahub.checkpoints import write_json_atomic


PHENOTYPE_SLUG_ALIASES = {
    "acute_myocardial_infarcti": "acute_myocardial_infarction",
    "aortic_coarctati": "aortic_coarctation",
    "aortic_stenosi": "aortic_stenosis",
    "aspartate_aminotransferase_level": "aspartate_aminotransferase_levels",
    "atherosclerosi": "atherosclerosis",
    "atrial_fibrillati": "atrial_fibrillation",
    "brain_infarcti": "brain_infarction",
    "cardiac_arre": "cardiac_arrest",
    "cardiodysrhythmic_potassium-sensitive_periodic_paralysi": (
        "cardiodysrhythmic_potassium-sensitive_periodic_paralysis"
    ),
    "cerebral_arteriovenous_malformati": "cerebral_arteriovenous_malformation",
    "cerebral_atherosclerosi": "cerebral_atherosclerosis",
    "cerebral_infarcti": "cerebral_infarction",
    "cervical_artery_dissecti": "cervical_artery_dissection",
    "coronary_atherosclerosi": "coronary_atherosclerosis",
    "coronary_stenosi": "coronary_stenosis",
    "cular_hypertensi": "ocular_hypertension",
    "e:a_rati": "e:a_ratio",
    "econdary_hypertensi": "secondary_hypertension",
    "ejection_fracti": "ejection_fraction",
    "electrolyte": "electrolytes",
    "endothelial_dysfuncti": "endothelial_dysfunction",
    "eurovascular_disease": "neurovascular_disease",
    "familial_pulmonary_arterial_hypertensi": "familial_pulmonary_arterial_hypertension",
    "genetic_hypertensi": "genetic_hypertension",
    "hemoglobin_level": "hemoglobin_levels",
    "hypertensi": "hypertension",
    "hypotensi": "hypotension",
    "inflammati": "inflammation",
    "left_ventricular_ma": "left_ventricular_mass",
    "left_ventricular_noncompacti": "left_ventricular_noncompaction",
    "mean_corpuscular_hemoglobi": "mean_corpuscular_hemoglobin",
    "mitral_valve_stenosi": "mitral_valve_stenosis",
    "myocardial_infarcti": "myocardial_infarction",
    "myocarditi": "myocarditis",
    "pericarditi": "pericarditis",
    "portal_hypertensi": "portal_hypertension",
    "primary_hypertensi": "primary_hypertension",
    "primary_pulmonary_hypertensi": "primary_pulmonary_hypertension",
    "pulmonary_arterial_hypertensi": "pulmonary_arterial_hypertension",
    "rthostatic_hypotensi": "orthostatic_hypotension",
    "triglyceride": "triglyceride_levels",
    "troke": "stroke",
    "ubarachnoid_hemorrhage": "subarachnoid_hemorrhage",
    "udden_cardiac_arrest": "sudden_cardiac_arrest",
    "upraventricular_tachycardia": "supraventricular_tachycardia",
    "vasculiti": "vasculitis",
    "ventricular_fibrillati": "ventricular_fibrillation",
    "ymptomatic_heart_failure": "symptomatic_heart_failure",
    "ystolic_heart_failure": "systolic_heart_failure",
}

DROP_PHENOTYPE_SLUGS = {"roke"}
DROP_GENE_PREFIXES = {"roke_"}


@dataclass
class PhenotypeRegistry:
    canonical_slugs: set[str]
    display_labels: dict[str, str]

    @classmethod
    def from_tree_json(cls, path: str | Path) -> "PhenotypeRegistry":
        payload = json.loads(Path(path).read_text())
        canonical_slugs: set[str] = set()
        display_labels: dict[str, str] = {}

        def register(label: str) -> None:
            slug = PhenotypeMapper.normalize(label)
            if not slug:
                return
            canonical_slugs.add(slug)
            display_labels.setdefault(slug, str(label).strip())

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                for label, child in node.items():
                    register(str(label))
                    walk(child)
                return
            if isinstance(node, list):
                for item in node:
                    if isinstance(item, dict):
                        walk(item)
                    else:
                        register(str(item))

        walk(payload)
        return cls(canonical_slugs=canonical_slugs, display_labels=display_labels)

    def resolve(self, value: str | None) -> str | None:
        slug = PhenotypeMapper.normalize(value)
        if not slug:
            return None
        if slug in DROP_PHENOTYPE_SLUGS:
            return None
        mapped = PHENOTYPE_SLUG_ALIASES.get(slug, slug)
        if mapped in DROP_PHENOTYPE_SLUGS:
            return None
        if mapped not in self.canonical_slugs:
            return mapped
        return mapped

    def display(self, slug: str) -> str:
        return slug.replace("_", " ")


@dataclass
class CleanupCheckpoint:
    path: Path
    completed: set[str] = field(default_factory=set)

    def load(self) -> None:
        if not self.path.exists():
            return
        payload = json.loads(self.path.read_text())
        completed = payload.get("completed", [])
        if isinstance(completed, list):
            self.completed = {str(item) for item in completed}

    def save(self) -> None:
        write_json_atomic(
            self.path,
            {
                "version": 1,
                "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                "completed": sorted(self.completed),
            },
            indent=2,
            sort_keys=True,
        )

    def reset(self) -> None:
        self.completed = set()
        if self.path.exists():
            self.path.unlink()

    def mark(self, key: str) -> None:
        self.completed.add(key)


@dataclass
class CleanupSummary:
    dry_run: bool
    hidden_raw_files_removed: int = 0
    phenotype_files_rewritten: int = 0
    phenotype_files_moved: int = 0
    phenotype_files_dropped: int = 0
    overall_files_rewritten: int = 0
    overall_files_dropped: int = 0
    rows_read: int = 0
    rows_written: int = 0
    unknown_phenotype_dirs: list[str] = field(default_factory=list)
    unknown_row_labels: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "hidden_raw_files_removed": self.hidden_raw_files_removed,
            "phenotype_files_rewritten": self.phenotype_files_rewritten,
            "phenotype_files_moved": self.phenotype_files_moved,
            "phenotype_files_dropped": self.phenotype_files_dropped,
            "overall_files_rewritten": self.overall_files_rewritten,
            "overall_files_dropped": self.overall_files_dropped,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "unknown_phenotype_dirs": sorted(set(self.unknown_phenotype_dirs)),
            "unknown_row_labels": sorted(set(self.unknown_row_labels)),
        }


def find_macos_hidden_files(raw_root: str | Path) -> list[Path]:
    root = Path(raw_root)
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and (path.name.startswith("._") or path.name == ".DS_Store")
    )


def canonicalize_variant_viewer_artifacts(
    *,
    variant_viewer_root: str | Path,
    phenotype_tree_json: str | Path,
    raw_root: str | Path | None = None,
    checkpoint_path: str | Path | None = None,
    report_path: str | Path | None = None,
    dry_run: bool = False,
    remove_hidden_raw: bool = False,
    reset_checkpoint: bool = False,
    fail_on_unknown: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    log = logger or logging.getLogger(__name__)
    root = Path(variant_viewer_root)
    registry = PhenotypeRegistry.from_tree_json(phenotype_tree_json)
    summary = CleanupSummary(dry_run=dry_run)
    checkpoint = CleanupCheckpoint(
        Path(checkpoint_path) if checkpoint_path else root / ".canonicalize_checkpoint.json"
    )
    if reset_checkpoint:
        checkpoint.reset()
    checkpoint.load()

    if remove_hidden_raw and raw_root:
        hidden_files = find_macos_hidden_files(raw_root)
        for path in hidden_files:
            log.info("Removing macOS hidden raw file: path=%s dry_run=%s", path, dry_run)
            if not dry_run:
                path.unlink(missing_ok=True)
            summary.hidden_raw_files_removed += 1

    _canonicalize_phenotype_dirs(root, registry, checkpoint, summary, dry_run=dry_run, log=log)
    _canonicalize_overall_files(root, registry, checkpoint, summary, dry_run=dry_run, log=log)
    if not dry_run:
        checkpoint.save()

    if fail_on_unknown and (summary.unknown_phenotype_dirs or summary.unknown_row_labels):
        payload = summary.as_dict()
        if report_path:
            write_json_atomic(report_path, payload, indent=2, sort_keys=True)
        raise ValueError(
            "Variant Viewer cleanup found unknown phenotypes: "
            f"dirs={sorted(set(summary.unknown_phenotype_dirs))} "
            f"rows={sorted(set(summary.unknown_row_labels))}"
        )

    payload = summary.as_dict()
    if report_path:
        write_json_atomic(report_path, payload, indent=2, sort_keys=True)
    return payload


def _canonicalize_phenotype_dirs(
    root: Path,
    registry: PhenotypeRegistry,
    checkpoint: CleanupCheckpoint,
    summary: CleanupSummary,
    *,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    phenotypes_root = root / "phenotypes"
    if not phenotypes_root.exists():
        return

    files = sorted(phenotypes_root.glob("*/*.csv"))
    total = len(files)
    for index, source_path in enumerate(files, start=1):
        key = f"phenotypes/{source_path.parent.name}/{source_path.name}"
        if key in checkpoint.completed and source_path.exists():
            continue
        if index == 1 or index % 1000 == 0 or index == total:
            log.info("Variant Viewer phenotype cleanup progress: files=%d/%d current=%s", index, total, source_path)

        source_slug = PhenotypeMapper.normalize(source_path.parent.name)
        target_slug = registry.resolve(source_slug)
        if target_slug is None or any(source_path.stem.startswith(prefix) for prefix in DROP_GENE_PREFIXES):
            summary.phenotype_files_dropped += 1
            if not dry_run:
                source_path.unlink(missing_ok=True)
                _remove_empty_parents(source_path.parent, stop_at=phenotypes_root)
            if not dry_run:
                checkpoint.mark(key)
            continue
        if target_slug not in registry.canonical_slugs:
            summary.unknown_phenotype_dirs.append(source_slug)
            if not dry_run:
                checkpoint.mark(key)
            continue

        rows, fieldnames = _read_csv_rows(source_path)
        canonical_rows, canonical_fields = _canonicalize_rows(rows, fieldnames, registry, summary)
        target_path = phenotypes_root / target_slug / source_path.name
        changed = target_path != source_path or canonical_rows != rows
        if changed:
            summary.phenotype_files_rewritten += 1
        if target_path != source_path:
            summary.phenotype_files_moved += 1
        if changed and not dry_run:
            _merge_write_csv(target_path, canonical_rows, canonical_fields)
            if source_path.exists() and source_path != target_path:
                source_path.unlink(missing_ok=True)
                _remove_empty_parents(source_path.parent, stop_at=phenotypes_root)
        if not dry_run:
            checkpoint.mark(key)


def _canonicalize_overall_files(
    root: Path,
    registry: PhenotypeRegistry,
    checkpoint: CleanupCheckpoint,
    summary: CleanupSummary,
    *,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    overall_root = root / "overall"
    if not overall_root.exists():
        return

    files = sorted(overall_root.glob("*.csv"))
    total = len(files)
    for index, source_path in enumerate(files, start=1):
        key = f"overall/{source_path.name}"
        if key in checkpoint.completed and source_path.exists():
            continue
        if index == 1 or index % 1000 == 0 or index == total:
            log.info("Variant Viewer overall cleanup progress: files=%d/%d current=%s", index, total, source_path)

        if any(source_path.stem.startswith(prefix) for prefix in DROP_GENE_PREFIXES):
            summary.overall_files_dropped += 1
            if not dry_run:
                source_path.unlink(missing_ok=True)
            if not dry_run:
                checkpoint.mark(key)
            continue

        rows, fieldnames = _read_csv_rows(source_path)
        canonical_rows, canonical_fields = _canonicalize_rows(rows, fieldnames, registry, summary)
        if not canonical_rows:
            summary.overall_files_dropped += 1
            if not dry_run:
                source_path.unlink(missing_ok=True)
            if not dry_run:
                checkpoint.mark(key)
            continue
        if canonical_rows != rows:
            summary.overall_files_rewritten += 1
            if not dry_run:
                _write_csv_atomic(source_path, canonical_rows, canonical_fields)
        if not dry_run:
            checkpoint.mark(key)


def _read_csv_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
        return rows, list(reader.fieldnames or [])


def _canonicalize_rows(
    rows: list[dict[str, str]],
    fieldnames: list[str],
    registry: PhenotypeRegistry,
    summary: CleanupSummary,
) -> tuple[list[dict[str, str]], list[str]]:
    fields = list(fieldnames)
    canonical_rows: list[dict[str, str]] = []
    for row in rows:
        summary.rows_read += 1
        cloned = dict(row)
        drop = False
        for column in ("phenotype", "source_phenotype"):
            if column not in cloned:
                continue
            raw_value = cloned.get(column)
            slug = registry.resolve(raw_value)
            if slug is None:
                drop = True
                break
            if slug not in registry.canonical_slugs:
                summary.unknown_row_labels.append(PhenotypeMapper.normalize(raw_value))
                continue
            cloned[column] = registry.display(slug)
        if drop:
            continue
        canonical_rows.append(cloned)
        summary.rows_written += 1
    return _deduplicate_rows(canonical_rows, fields), fields


def _deduplicate_rows(rows: list[dict[str, str]], fieldnames: list[str]) -> list[dict[str, str]]:
    seen: set[tuple[str, ...]] = set()
    unique_rows: list[dict[str, str]] = []
    for row in rows:
        key = tuple(str(row.get(field, "")) for field in fieldnames)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def _merge_write_csv(target_path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    merged_rows = list(rows)
    merged_fields = list(fieldnames)
    if target_path.exists():
        existing_rows, existing_fields = _read_csv_rows(target_path)
        merged_rows = existing_rows + merged_rows
        merged_fields = _merge_fieldnames(existing_fields, merged_fields)
    _write_csv_atomic(target_path, _deduplicate_rows(merged_rows, merged_fields), merged_fields)


def _merge_fieldnames(*field_sets: Iterable[str]) -> list[str]:
    merged: list[str] = []
    for fields in field_sets:
        for field in fields:
            if field and field not in merged:
                merged.append(field)
    return merged


def _write_csv_atomic(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temp_path.replace(path)


def _remove_empty_parents(path: Path, *, stop_at: Path) -> None:
    current = path
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def backup_tree(source: str | Path, backup_root: str | Path) -> Path:
    source_path = Path(source)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = Path(backup_root) / f"{source_path.name}.{timestamp}"
    shutil.copytree(source_path, backup_path)
    return backup_path
