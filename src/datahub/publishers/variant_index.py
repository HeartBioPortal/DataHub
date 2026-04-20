"""Variant-level association index publisher for filtered chart aggregation."""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from datahub.axis_normalization import is_unknown_axis_value, normalize_axis_value
from datahub.export_manifest import AssociationExportRuntime
from datahub.models import CanonicalRecord
from datahub.phenotype_paths import PhenotypePathResolver
from datahub.publishers.base import Publisher


class VariantIndexPublisher(Publisher):
    """Publish filterable per-gene variant records for association charts.

    The legacy association payload stores phenotype-level counters. Those counters
    are intentionally compact, but they cannot be safely summed across arbitrary
    phenotype filters because the same rsID can appear in multiple phenotype
    buckets. This publisher preserves the variant identity needed to re-run the
    same variant-centric aggregation rule used by overall publication.
    """

    def __init__(
        self,
        *,
        output_root: str | Path,
        skip_unknown_axis_values: bool = True,
        ancestry_value_precision: int | None = None,
        json_indent: int | None = 4,
        json_compression: str = "none",
        json_gzip_level: int = 6,
        tree_json_path: str | Path | None = None,
        export_runtime: AssociationExportRuntime | None = None,
    ) -> None:
        self.output_root = Path(output_root)
        self.skip_unknown_axis_values = skip_unknown_axis_values
        self.ancestry_value_precision = ancestry_value_precision
        self.json_indent = json_indent
        self.json_compression = str(json_compression).strip().lower()
        self.json_gzip_level = int(json_gzip_level)
        self.path_resolver = (
            PhenotypePathResolver.from_tree_json(tree_json_path)
            if tree_json_path
            else None
        )
        self.export_runtime = export_runtime
        if self.json_compression not in {"none", "gzip"}:
            raise ValueError("json_compression must be one of: none, gzip")

    def publish(self, records: Iterable[CanonicalRecord]) -> None:
        grouped: dict[str, dict[str, dict[tuple[tuple[str, ...], str], list[CanonicalRecord]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for record in records:
            dataset_type = (record.dataset_type or "CVD").upper()
            if not record.variant_id or not str(record.variant_id).strip():
                continue
            label_path = self._resolve_label_path(record, dataset_type)
            if not label_path:
                continue
            key = (label_path, str(record.variant_id).strip())
            grouped[dataset_type][record.gene_id][key].append(record)

        for dataset_type, genes in grouped.items():
            variant_dir = (
                self.output_root / "association" / "final" / "variant_index" / dataset_type
            )
            variant_dir.mkdir(parents=True, exist_ok=True)

            for gene_id, variant_groups in genes.items():
                payload = [
                    self._build_variant_entry(
                        dataset_type=dataset_type,
                        gene_id=gene_id,
                        label_path=label_path,
                        records=variant_records,
                    )
                    for label_path, _variant_id in sorted(
                        variant_groups.keys(),
                        key=lambda item: (item[0], item[1]),
                    )
                    for variant_records in [variant_groups[(label_path, _variant_id)]]
                ]
                self._write_payload(variant_dir / f"{self._safe_gene(gene_id)}.json", payload)

    def _build_variant_entry(
        self,
        *,
        dataset_type: str,
        gene_id: str,
        label_path: tuple[str, ...],
        records: list[CanonicalRecord],
    ) -> dict[str, Any]:
        representative = self._select_best_record(records)
        label_key = "trait" if dataset_type == "TRAIT" else "disease"
        entry: dict[str, Any] = {
            "variant_id": representative.variant_id,
            "gene_id": gene_id,
            "dataset_type": dataset_type,
            "phenotype": representative.phenotype,
            "phenotype_path": list(label_path),
            "label_key": label_key,
            label_key: list(label_path),
        }

        for output_name, axis, value in (
            ("variation_type", "variation", representative.variation_type),
            (
                "most_severe_consequence",
                "most_severe_consequence",
                representative.most_severe_consequence,
            ),
            (
                "clinical_significance",
                "clinical_significance",
                representative.clinical_significance,
            ),
        ):
            normalized = self._normalize_axis(axis=axis, value=value)
            if normalized is not None:
                entry[output_name] = normalized
            elif not self.skip_unknown_axis_values:
                entry[output_name] = "Unknown"

        if representative.p_value is not None:
            entry["p_value"] = representative.p_value

        ancestry = self._merge_ancestry(records)
        if ancestry:
            entry["ancestry"] = ancestry

        sources = sorted({record.source for record in records if record.source})
        if sources:
            entry["sources"] = sources
            entry["source"] = representative.source

        metadata = self._compact_metadata(records)
        if metadata:
            entry["metadata"] = metadata

        return entry

    def _resolve_label_path(
        self,
        record: CanonicalRecord,
        dataset_type: str,
    ) -> tuple[str, ...]:
        fallback_path = [record.disease_category, record.phenotype]
        if self.path_resolver is None:
            if self.export_runtime is None:
                return tuple(
                    str(segment).strip()
                    for segment in fallback_path
                    if segment is not None and str(segment).strip()
                )
            return self.export_runtime.resolve_label_path(
                record=record,
                dataset_type=dataset_type,
            )

        if self.export_runtime is not None:
            return self.export_runtime.resolve_label_path(
                record=record,
                dataset_type=dataset_type,
            )

        return self.path_resolver.resolve_leaf_path(
            dataset_type=dataset_type,
            phenotype=record.phenotype,
            fallback_category=record.disease_category,
            fallback_path=fallback_path,
        )

    def _select_best_record(self, records: list[CanonicalRecord]) -> CanonicalRecord:
        if not records:
            raise ValueError("variant index entry requires at least one record")
        best = records[0]
        for candidate in records[1:]:
            best = self._pick_best_record(best, candidate)
        return best

    def _normalize_axis(self, *, axis: str, value: Any) -> str | None:
        if self.export_runtime is not None:
            return self.export_runtime.normalize_axis(axis=axis, value=value)
        return normalize_axis_value(value, axis=axis)

    def _merge_ancestry(self, records: list[CanonicalRecord]) -> dict[str, Any]:
        ancestry: dict[str, Any] = {}
        for record in records:
            for population, value in record.ancestry.items():
                if self._is_unknown(value):
                    continue
                ancestry[str(population)] = self._normalize_ancestry_value(value)
        return dict(sorted(ancestry.items(), key=lambda item: item[0]))

    @staticmethod
    def _compact_metadata(records: list[CanonicalRecord]) -> dict[str, Any]:
        source_files = sorted(
            {
                str(record.metadata.get("source_file"))
                for record in records
                if record.metadata.get("source_file")
            }
        )
        phenotype_keys = sorted(
            {
                str(record.metadata.get("phenotype_key"))
                for record in records
                if record.metadata.get("phenotype_key")
            }
        )
        metadata: dict[str, Any] = {}
        if source_files:
            metadata["source_files"] = source_files
        if phenotype_keys:
            metadata["phenotype_keys"] = phenotype_keys
        return metadata

    def _write_payload(self, base_path: Path, payload: Any) -> None:
        base_path.parent.mkdir(parents=True, exist_ok=True)
        gz_path = base_path.with_suffix(base_path.suffix + ".gz")

        if self.json_compression == "gzip":
            with gzip.open(gz_path, "wt", encoding="utf-8", compresslevel=self.json_gzip_level) as stream:
                json.dump(
                    payload,
                    stream,
                    indent=self.json_indent,
                    separators=(",", ":") if self.json_indent is None else None,
                )
            if base_path.exists():
                base_path.unlink()
            return

        with base_path.open("w") as stream:
            json.dump(
                payload,
                stream,
                indent=self.json_indent,
                separators=(",", ":") if self.json_indent is None else None,
            )
        if gz_path.exists():
            gz_path.unlink()

    @staticmethod
    def _pick_best_record(existing: CanonicalRecord, candidate: CanonicalRecord) -> CanonicalRecord:
        existing_p = existing.p_value
        candidate_p = candidate.p_value
        if existing_p is None:
            return candidate
        if candidate_p is None:
            return existing
        return candidate if candidate_p < existing_p else existing

    @staticmethod
    def _safe_gene(gene_id: str) -> str:
        return gene_id.replace("/", "-")

    @staticmethod
    def _is_unknown(value: Any) -> bool:
        return is_unknown_axis_value(value)

    def _normalize_ancestry_value(self, value: Any) -> Any:
        if self.ancestry_value_precision is None:
            return value

        try:
            return round(float(value), self.ancestry_value_precision)
        except (TypeError, ValueError):
            return value


class VariantIndexStreamWriter:
    """Write variant-index payloads incrementally for memory-safe backfills."""

    def __init__(
        self,
        *,
        output_root: str | Path,
        skip_unknown_axis_values: bool = True,
        ancestry_value_precision: int | None = None,
        json_indent: int | None = 4,
        json_compression: str = "none",
        json_gzip_level: int = 6,
        tree_json_path: str | Path | None = None,
        export_runtime: AssociationExportRuntime | None = None,
    ) -> None:
        self._formatter = VariantIndexPublisher(
            output_root=output_root,
            skip_unknown_axis_values=skip_unknown_axis_values,
            ancestry_value_precision=ancestry_value_precision,
            json_indent=json_indent,
            json_compression=json_compression,
            json_gzip_level=json_gzip_level,
            tree_json_path=tree_json_path,
            export_runtime=export_runtime,
        )
        self.output_root = Path(output_root)
        self.json_indent = json_indent
        self.json_compression = str(json_compression).strip().lower()
        self.json_gzip_level = int(json_gzip_level)
        self._current_key: tuple[str, str] | None = None
        self._stream: Any | None = None
        self._wrote_entry = False

    def write_record(self, record: CanonicalRecord) -> None:
        dataset_type = (record.dataset_type or "CVD").upper()
        gene_id = record.gene_id
        label_path = self._formatter._resolve_label_path(record, dataset_type)
        if not label_path:
            return

        self._ensure_open(dataset_type=dataset_type, gene_id=gene_id)
        if self._stream is None:
            raise RuntimeError("variant index stream was not opened")

        entry = self._formatter._build_variant_entry(
            dataset_type=dataset_type,
            gene_id=gene_id,
            label_path=label_path,
            records=[record],
        )
        if self._wrote_entry:
            self._stream.write(",")
            if self.json_indent is not None:
                self._stream.write("\n")
        elif self.json_indent is not None:
            self._stream.write("\n")

        json.dump(
            entry,
            self._stream,
            indent=self.json_indent,
            separators=(",", ":") if self.json_indent is None else None,
        )
        self._wrote_entry = True

    def close(self) -> None:
        if self._stream is None:
            return
        if self._wrote_entry and self.json_indent is not None:
            self._stream.write("\n")
        self._stream.write("]")
        self._stream.close()
        self._stream = None
        self._current_key = None
        self._wrote_entry = False

    def _ensure_open(self, *, dataset_type: str, gene_id: str) -> None:
        key = (dataset_type, gene_id)
        if self._current_key == key:
            return
        self.close()

        base_path = (
            self.output_root
            / "association"
            / "final"
            / "variant_index"
            / dataset_type
            / f"{self._formatter._safe_gene(gene_id)}.json"
        )
        base_path.parent.mkdir(parents=True, exist_ok=True)
        gz_path = base_path.with_suffix(base_path.suffix + ".gz")

        if self.json_compression == "gzip":
            self._stream = gzip.open(
                gz_path,
                "wt",
                encoding="utf-8",
                compresslevel=self.json_gzip_level,
            )
            if base_path.exists():
                base_path.unlink()
        else:
            self._stream = base_path.open("w")
            if gz_path.exists():
                gz_path.unlink()

        self._stream.write("[")
        self._current_key = key
        self._wrote_entry = False
