"""Legacy-compatible structural-variant JSON publisher."""

from __future__ import annotations

import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

from datahub.artifact_io import load_json_artifact
from datahub.checkpoints import StructuralVariantCheckpoint, write_json_atomic
from datahub.models import CanonicalRecord
from datahub.output_contracts import OutputContractLoader
from datahub.publishers.base import Publisher


logger = logging.getLogger(__name__)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_int(value: Any) -> int | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _variant_region_sort_key(variant: dict[str, Any]) -> tuple[int, int, str]:
    region = _clean_text(variant.get("variant_region"))
    start = 0
    end = 0
    if "-" in region:
        start_text, end_text = region.split("-", 1)
        start = _parse_int(start_text) or 0
        end = _parse_int(end_text) or 0
    return (start, end, _clean_text(variant.get("variant_id")))


def _variant_identity(variant: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        _clean_text(variant.get("variant_id")),
        _clean_text(variant.get("study_id")),
        _clean_text(variant.get("variant_region")),
        _clean_text(variant.get("variant_type")),
    )


def load_structural_variant_payload(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload_path = Path(path)
    if not payload_path.exists():
        return {}
    loaded = load_json_artifact(payload_path)
    return loaded if isinstance(loaded, dict) else {}


def _sort_payload_variants_in_place(payload: dict[str, Any]) -> None:
    for gene_payload in payload.values():
        variants = gene_payload.get("variants")
        if variants:
            variants.sort(key=_variant_region_sort_key)


def merge_structural_variant_payloads(
    base_payload: dict[str, Any],
    incoming_payload: dict[str, Any],
) -> dict[str, Any]:
    merged = deepcopy(base_payload)
    _merge_structural_variant_payloads_in_place(merged, incoming_payload)
    return merged


def _merge_structural_variant_payloads_in_place(
    target_payload: dict[str, Any],
    incoming_payload: dict[str, Any],
    *,
    seen_variant_identities: dict[str, set[tuple[str, str, str, str]]] | None = None,
    sort_variants: bool = True,
) -> None:
    local_seen = seen_variant_identities if seen_variant_identities is not None else {}

    for gene_name, incoming_gene in incoming_payload.items():
        target = target_payload.setdefault(gene_name, {})
        for field_name in ("gene_location", "strand", "biotype"):
            if target.get(field_name) in (None, "") and incoming_gene.get(field_name) not in (None, ""):
                target[field_name] = deepcopy(incoming_gene[field_name])

        if not target.get("canonical_transcript") and incoming_gene.get("canonical_transcript"):
            target["canonical_transcript"] = deepcopy(incoming_gene["canonical_transcript"])

        existing_variants = target.get("variants")
        if not isinstance(existing_variants, list):
            existing_variants = []
            target["variants"] = existing_variants

        seen = local_seen.get(gene_name)
        if seen is None:
            seen = {_variant_identity(variant) for variant in existing_variants}
            local_seen[gene_name] = seen
        for variant in incoming_gene.get("variants") or []:
            identity = _variant_identity(variant)
            if identity in seen:
                continue
            existing_variants.append(deepcopy(variant))
            seen.add(identity)

    if sort_variants:
        _sort_payload_variants_in_place(target_payload)


def _seed_seen_variant_identities(
    payload: dict[str, Any],
) -> dict[str, set[tuple[str, str, str, str]]]:
    seeded: dict[str, set[tuple[str, str, str, str]]] = {}
    for gene_name, gene_payload in payload.items():
        variants = gene_payload.get("variants")
        if not isinstance(variants, list):
            continue
        seeded[gene_name] = {_variant_identity(variant) for variant in variants}
    return seeded


class StructuralVariantLegacyPublisher(Publisher):
    """Publish canonical structural-variant records into the legacy SV JSON contract."""

    def __init__(
        self,
        *,
        output_path: str | Path,
        report_path: str | Path | None = None,
        merge_source_json_path: str | Path | None = None,
        existing_json_path: str | Path | None = None,
        contract_path: str | Path | None = None,
        merge_existing: bool = False,
        json_indent: int | None = 2,
        progress_every: int = 5_000,
        checkpoint: StructuralVariantCheckpoint | None = None,
        checkpoint_every_rows: int = 50_000,
    ) -> None:
        self.output_path = Path(output_path)
        self.report_path = Path(report_path) if report_path else None
        merge_source = merge_source_json_path or existing_json_path
        if merge_existing and not merge_source:
            merge_source = self.output_path
        self.merge_source_json_path = Path(merge_source) if merge_source else None
        self.merge_existing = merge_existing
        self.json_indent = json_indent
        self.progress_every = max(int(progress_every), 1)
        self.checkpoint = checkpoint
        self.checkpoint_every_rows = max(int(checkpoint_every_rows), 1)
        self.publish_report: dict[str, Any] = {}
        self.contract = OutputContractLoader().load(contract_path or "structural_variant_legacy")
        self._active_payload: dict[str, Any] | None = None
        self._latest_completed_rows: dict[str, int] = {}

    def publish(self, records: Iterable[CanonicalRecord]) -> None:
        payload: dict[str, Any]
        if self.merge_existing:
            payload = load_structural_variant_payload(self.merge_source_json_path)
        else:
            payload = {}
        seen_variant_identities = _seed_seen_variant_identities(payload)
        records_seen = 0
        records_skipped = 0
        self._active_payload = payload
        self._latest_completed_rows = {}

        try:
            for record in records:
                records_seen += 1
                applied = self._apply_record(
                    payload=payload,
                    seen_variant_identities=seen_variant_identities,
                    record=record,
                )
                if not applied:
                    records_skipped += 1
                    self._log_progress(
                        records_seen=records_seen,
                        genes_written=len(payload),
                        record=record,
                    )
                    continue

                self._log_progress(
                    records_seen=records_seen,
                    genes_written=len(payload),
                    record=record,
                )
        finally:
            self._active_payload = None

        _sort_payload_variants_in_place(payload)
        self._write_payload_snapshot(payload, sort_variants=False)
        self._persist_final_checkpoint()

        self.publish_report = {
            "output_path": str(self.output_path),
            "contract_name": self.contract.name,
            "records_seen": records_seen,
            "records_skipped": records_skipped,
            "genes_written": len(payload),
            "variants_written": sum(
                len(list(gene_payload.get("variants") or []))
                for gene_payload in payload.values()
            ),
        }

        if self.report_path is not None:
            write_json_atomic(self.report_path, self.publish_report, indent=2, sort_keys=True)

        logger.info(
            "Wrote %d genes / %d variants to %s",
            self.publish_report["genes_written"],
            self.publish_report["variants_written"],
            self.output_path,
        )

    def _write_payload_snapshot(self, payload: dict[str, Any], *, sort_variants: bool) -> None:
        snapshot_payload = payload
        if sort_variants:
            snapshot_payload = deepcopy(payload)
            _sort_payload_variants_in_place(snapshot_payload)
        write_json_atomic(self.output_path, snapshot_payload, indent=self.json_indent, sort_keys=True)

    def mark_source_row_completed(self, source_file_key: str, row_index: int) -> None:
        if not source_file_key or row_index <= 0:
            return
        self._latest_completed_rows[source_file_key] = row_index
        if self.checkpoint is None or self._active_payload is None:
            return
        if row_index % self.checkpoint_every_rows != 0:
            return
        self._write_payload_snapshot(self._active_payload, sort_variants=False)
        self.checkpoint.mark_completed_row(
            source_file=source_file_key,
            row_index=row_index,
            output_path=self.output_path,
        )

    def _persist_final_checkpoint(self) -> None:
        if self.checkpoint is None:
            return
        for source_file_key, row_index in sorted(self._latest_completed_rows.items()):
            self.checkpoint.mark_completed_row(
                source_file=source_file_key,
                row_index=row_index,
                output_path=self.output_path,
            )

    def _log_progress(
        self,
        *,
        records_seen: int,
        genes_written: int,
        record: CanonicalRecord | None = None,
    ) -> None:
        if records_seen % self.progress_every != 0:
            return

        row_progress = ""
        if record is not None:
            metadata = dict(record.metadata or {})
            source_row_index = _parse_int(metadata.get("source_row_index"))
            source_total_rows = _parse_int(metadata.get("source_total_rows"))
            if source_row_index is not None and source_total_rows:
                row_progress = (
                    f" rows={source_row_index}/{source_total_rows} "
                    f"({source_row_index / source_total_rows * 100.0:.1f}%)"
                )
            elif source_row_index is not None:
                row_progress = f" rows={source_row_index}/?"

        logger.info(
            "[SV publish] records=%d genes=%d%s",
            records_seen,
            genes_written,
            row_progress,
        )

    def _apply_record(
        self,
        *,
        payload: dict[str, Any],
        seen_variant_identities: dict[str, set[tuple[str, str, str, str]]],
        record: CanonicalRecord,
    ) -> bool:
        gene_name = _clean_text(record.gene_id)
        metadata = dict(record.metadata or {})
        variant_region = _clean_text(metadata.get("variant_region"))
        study_id = _clean_text(metadata.get("study_id"))
        if not gene_name or not record.variant_id or not variant_region or not study_id:
            return False

        gene_record = payload.get(gene_name)
        if not isinstance(gene_record, dict):
            gene_record = deepcopy(self.contract.payload["gene_defaults"])
            gene_record["variants"] = []
            payload[gene_name] = gene_record

        for field_name, field_value in (
            ("gene_location", metadata.get("gene_location")),
            ("strand", metadata.get("strand")),
            ("biotype", metadata.get("biotype")),
        ):
            if gene_record.get(field_name) in (None, "") and field_value not in (None, ""):
                gene_record[field_name] = field_value

        if not gene_record.get("canonical_transcript") and metadata.get("canonical_transcript"):
            gene_record["canonical_transcript"] = deepcopy(metadata.get("canonical_transcript") or [])

        existing_variants = gene_record.get("variants")
        if not isinstance(existing_variants, list):
            existing_variants = []
            gene_record["variants"] = existing_variants

        identity = (
            _clean_text(record.variant_id),
            study_id,
            variant_region,
            _clean_text(record.variation_type),
        )
        seen = seen_variant_identities.setdefault(gene_name, set())
        if identity in seen:
            return True

        variant_payload = deepcopy(self.contract.payload["variant_defaults"])
        variant_payload.update(
            {
                "variant_id": record.variant_id,
                "study_id": study_id,
                "variant_type": record.variation_type,
                "phenotype": list(metadata.get("phenotypes") or []),
                "clinical_significance": record.clinical_significance,
                "assembly_name": metadata.get("assembly_name"),
                "variant_region": variant_region,
            }
        )
        existing_variants.append(variant_payload)
        seen.add(identity)
        return True
