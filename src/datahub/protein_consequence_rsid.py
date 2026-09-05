"""Build rsID-preserving protein consequence payloads for HeartBioPortal.

The legacy protein viewer flattened association and annotation records into CSV
rows and then deduplicated without rsID.  This module keeps association context
and transcript-level VEP annotation as separate collections.  Display grouping
is deliberately left to the API/UI so no variant identity is discarded here.
"""

from __future__ import annotations

import concurrent.futures
import gzip
import hashlib
import json
import logging
import multiprocessing
import re
import shutil
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb


SCHEMA_VERSION = "protein_consequence_rsid_v2.0"
ANNOTATION_CONTRACT = "ensembl_vep_transcript_consequence_v1"
RSID_RE = re.compile(r"^rs[0-9]+$", re.IGNORECASE)
HGVS_PROTEIN_RE = re.compile(
    r"(?:^|:)p\.([A-Za-z]{3}|[A-Z*])(\d+)([A-Za-z]{3}|[A-Z*=])"
)
AA3_TO_1 = {
    "Ala": "A", "Arg": "R", "Asn": "N", "Asp": "D", "Cys": "C",
    "Glu": "E", "Gln": "Q", "Gly": "G", "His": "H", "Ile": "I",
    "Leu": "L", "Lys": "K", "Met": "M", "Phe": "F", "Pro": "P",
    "Ser": "S", "Thr": "T", "Trp": "W", "Tyr": "Y", "Val": "V",
    "Ter": "*", "Sec": "U", "Pyl": "O",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def recorded_or_computed_sha256(path: Path) -> str:
    checksum_path = path.with_suffix(path.suffix + ".sha256")
    if checksum_path.is_file():
        value = checksum_path.read_text(encoding="utf-8").split()[0].strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", value):
            return value
    return sha256_file(path)


def stable_id(prefix: str, *parts: Any) -> str:
    text = "\x1f".join("" if value is None else str(value) for value in parts)
    return f"{prefix}:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def normalize_rsid(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text if RSID_RE.fullmatch(text) else None


def normalize_path(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(part).strip() for part in value if str(part).strip()]


def parse_hgvs_protein(value: Any) -> tuple[str | None, int | None]:
    text = str(value or "").strip()
    match = HGVS_PROTEIN_RE.search(text)
    if not match:
        return None, None
    before, position, after = match.groups()
    before = AA3_TO_1.get(before, before)
    after = AA3_TO_1.get(after, after)
    return f"{before}/{after}", int(position)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def write_deterministic_json_gz(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            zipped.write(encoded)
    temporary.replace(path)


@dataclass(frozen=True)
class BuildInputs:
    variant_index_root: Path
    vep_index_path: Path
    output_root: Path


_WORKER_BUILDER: Any = None
_WORKER_CONNECTION: Any = None


def _initialize_build_worker(inputs: BuildInputs, progress_interval: int) -> None:
    global _WORKER_BUILDER, _WORKER_CONNECTION
    _WORKER_BUILDER = ProteinConsequenceRsidBuilder(
        inputs,
        progress_interval=progress_interval,
    )
    _WORKER_CONNECTION = duckdb.connect(str(inputs.vep_index_path), read_only=True)


def _build_gene_worker(item: tuple[int, str]) -> tuple[int, str, dict[str, Any]]:
    if _WORKER_BUILDER is None or _WORKER_CONNECTION is None:
        raise RuntimeError("Protein consequence worker was not initialized")
    index, gene = item
    started = time.monotonic()
    result = _WORKER_BUILDER.build_gene(_WORKER_CONNECTION, gene)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return index, gene, result


class ProteinConsequenceRsidBuilder:
    """Create compact per-gene payloads without collapsing rsIDs/transcripts."""

    def __init__(
        self,
        inputs: BuildInputs,
        *,
        logger: logging.Logger | None = None,
        progress_interval: int = 25,
        workers: int = 1,
    ) -> None:
        self.inputs = inputs
        self.logger = logger or logging.getLogger(__name__)
        self.progress_interval = max(1, int(progress_interval))
        self.workers = max(1, int(workers))
        self.genes_root = inputs.output_root / "genes"
        self.checkpoint_path = inputs.output_root / "build-checkpoint.json"
        self.manifest_path = inputs.output_root / "manifest.json"

    @staticmethod
    def create_vep_index(
        annotations_csv: Path,
        output_db: Path,
        *,
        reset: bool = False,
        threads: int = 4,
        memory_limit: str = "16GB",
        temp_directory: Path | None = None,
        source_sha256: str | None = None,
        logger: logging.Logger | None = None,
    ) -> dict[str, Any]:
        """Import all VEP rows once; every transcript/allele row is retained."""

        log = logger or logging.getLogger(__name__)
        annotations_csv = annotations_csv.resolve()
        output_db = output_db.resolve()
        output_db.parent.mkdir(parents=True, exist_ok=True)
        manifest_path = output_db.parent / f"{output_db.name}.manifest.json"
        if reset and output_db.exists():
            output_db.unlink()
        started = time.monotonic()
        connection = duckdb.connect(str(output_db))
        try:
            connection.execute(f"SET threads={max(1, int(threads))}")
            connection.execute(f"SET memory_limit='{memory_limit}'")
            connection.execute("PRAGMA enable_progress_bar")
            if temp_directory:
                temp_directory.mkdir(parents=True, exist_ok=True)
                escaped_temp = str(temp_directory.resolve()).replace("'", "''")
                connection.execute(f"SET temp_directory='{escaped_temp}'")
            existing = {
                row[0]
                for row in connection.execute("SHOW TABLES").fetchall()
            }
            if "vep_annotations" in existing:
                if not manifest_path.is_file():
                    raise ValueError("Existing VEP index has no manifest; use --reset")
                previous = json.loads(manifest_path.read_text(encoding="utf-8"))
                if previous.get("source_path") != str(annotations_csv):
                    raise ValueError("Existing VEP index was built from a different source path; use --reset")
                if int(previous.get("source_size_bytes") or -1) != annotations_csv.stat().st_size:
                    raise ValueError("Existing VEP index source size differs; use --reset")
                if source_sha256 and previous.get("source_sha256") != source_sha256:
                    raise ValueError("Existing VEP index source checksum differs; use --reset")
                columns = {row[1] for row in connection.execute("PRAGMA table_info('vep_annotations')").fetchall()}
                required = {"source_row_number", "rsid", "gene", "annotation_id"}
                if not required.issubset(columns):
                    raise ValueError("Existing VEP index uses an older schema; use --reset")
            else:
                log.info("Importing VEP annotations: source=%s output=%s", annotations_csv, output_db)
                connection.execute(
                    """
                    CREATE TABLE vep_annotations AS
                    WITH source AS (
                      SELECT row_number() OVER () AS source_row_number, *
                      FROM read_csv(?, header=true, all_varchar=true, ignore_errors=false)
                    )
                    SELECT
                      source_row_number,
                      lower(trim(rsid)) AS rsid,
                      upper(trim(gene)) AS gene,
                      nullif(trim(transcript), '') AS transcript_id,
                      nullif(trim(ENSP), '') AS protein_id,
                      try_cast(protein_length AS INTEGER) AS protein_length,
                      try_cast(protein_pos_start AS INTEGER) AS protein_position_start,
                      try_cast(protein_pos_end AS INTEGER) AS protein_position_end,
                      nullif(trim(HGVSc), '') AS hgvs_coding,
                      nullif(trim(HGVSp), '') AS hgvs_protein,
                      nullif(trim(Consequence), '') AS consequence,
                      nullif(trim(BIOTYPE), '') AS biotype,
                      nullif(trim(MANE_SELECT), '') AS mane_select,
                      nullif(trim(chromosome), '') AS chromosome,
                      try_cast(position AS BIGINT) AS genomic_position,
                      nullif(trim(ref), '') AS ref_allele,
                      nullif(trim(alt), '') AS alt_allele,
                      nullif(trim(variant_id), '') AS genomic_variant_id,
                      nullif(trim(CAF_ref), '') AS caf_ref_raw,
                      nullif(trim(CAF_alt), '') AS caf_alt_raw,
                      sha256(concat_ws(chr(31), cast(source_row_number AS VARCHAR),
                        lower(trim(rsid)), upper(trim(gene)),
                        coalesce(trim(transcript), ''), coalesce(trim(ENSP), ''),
                        coalesce(trim(HGVSp), ''), coalesce(trim(Consequence), ''),
                        coalesce(trim(chromosome), ''), coalesce(trim(position), ''),
                        coalesce(trim(ref), ''), coalesce(trim(alt), ''))) AS annotation_id
                    FROM source
                    WHERE regexp_full_match(lower(trim(rsid)), 'rs[0-9]+')
                      AND nullif(trim(gene), '') IS NOT NULL
                    """,
                    [str(annotations_csv)],
                )
                connection.execute("CHECKPOINT")
            log.info("Ensuring VEP lookup indexes")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_vep_gene_rsid ON vep_annotations(gene, rsid)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_vep_rsid ON vep_annotations(rsid)")
            connection.execute("CHECKPOINT")
            counts = connection.execute(
                """SELECT count(*), count(DISTINCT rsid), count(DISTINCT gene)
                   FROM vep_annotations"""
            ).fetchone()
            metadata = {
                "schema_version": SCHEMA_VERSION,
                "annotation_contract": ANNOTATION_CONTRACT,
                "annotation_source": "Ensembl VEP",
                "annotation_version": "114",
                "genome_assembly": "GRCh38",
                "source_path": str(annotations_csv),
                "source_size_bytes": annotations_csv.stat().st_size,
                "source_sha256": source_sha256,
                "output_path": str(output_db),
                "built_at": utc_now(),
                "rows": int(counts[0]),
                "distinct_rsids": int(counts[1]),
                "distinct_genes": int(counts[2]),
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
            manifest_path.write_text(
                json.dumps(metadata, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return metadata
        finally:
            connection.close()

    def discover_genes(self) -> list[str]:
        genes: set[str] = set()
        for dataset_type in ("CVD", "TRAIT"):
            root = self.inputs.variant_index_root / dataset_type
            if root.exists():
                genes.update(path.name.removesuffix(".json.gz").upper() for path in root.glob("*.json.gz"))
        return sorted(genes)

    def _load_checkpoint(self) -> dict[str, Any]:
        if not self.checkpoint_path.exists():
            return {"completed_genes": {}, "started_at": utc_now()}
        return json.loads(self.checkpoint_path.read_text(encoding="utf-8"))

    def _write_checkpoint(self, state: dict[str, Any]) -> None:
        state["updated_at"] = utc_now()
        temporary = self.checkpoint_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self.checkpoint_path)

    def reset(self) -> None:
        if self.inputs.output_root.exists():
            shutil.rmtree(self.inputs.output_root)

    def _variant_index_path(self, dataset_type: str, gene: str) -> Path:
        return self.inputs.variant_index_root / dataset_type / f"{gene}.json.gz"

    def _association_contexts(self, gene: str) -> tuple[dict[str, list[dict[str, Any]]], int]:
        contexts: dict[str, dict[tuple[str, str, str], dict[str, Any]]] = defaultdict(dict)
        input_rows = 0
        for dataset_type in ("CVD", "TRAIT"):
            path = self._variant_index_path(dataset_type, gene)
            if not path.exists():
                continue
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                rows = json.load(handle)
            if not isinstance(rows, list):
                raise ValueError(f"Variant index is not a list: {path}")
            for row in rows:
                input_rows += 1
                rsid = normalize_rsid(row.get("variant_id"))
                if not rsid:
                    continue
                phenotype_path = normalize_path(row.get("phenotype_path"))
                path_key = " > ".join(phenotype_path)
                source_values = row.get("sources") or [row.get("source") or "unknown"]
                if not isinstance(source_values, list):
                    source_values = [source_values]
                for source_value in source_values:
                    source = str(source_value or "unknown").strip()
                    key = (dataset_type, path_key, source)
                    current = contexts[rsid].get(key)
                    p_value = row.get("p_value")
                    try:
                        p_value = float(p_value) if p_value is not None else None
                    except (TypeError, ValueError):
                        p_value = None
                    if current is None:
                        contexts[rsid][key] = {
                            "dataset_type": dataset_type,
                            "phenotype": str(row.get("phenotype") or (phenotype_path[-1] if phenotype_path else "")),
                            "phenotype_path": phenotype_path,
                            "phenotype_path_key": path_key,
                            "source": source,
                            "minimum_reported_p_value": p_value,
                            "source_summary_count": 1,
                        }
                    else:
                        current["source_summary_count"] += 1
                        values = [value for value in (current["minimum_reported_p_value"], p_value) if value is not None]
                        current["minimum_reported_p_value"] = min(values) if values else None
        output = {
            rsid: sorted(values.values(), key=lambda item: (
                item["dataset_type"], item["phenotype_path_key"], item["source"]
            ))
            for rsid, values in contexts.items()
        }
        return output, input_rows

    def _annotations(self, connection: duckdb.DuckDBPyConnection, gene: str, rsids: Iterable[str]) -> list[dict[str, Any]]:
        values = sorted(set(rsids))
        if not values:
            return []
        connection.execute("CREATE OR REPLACE TEMP TABLE target_rsids(rsid VARCHAR)")
        connection.executemany("INSERT INTO target_rsids VALUES (?)", [(value,) for value in values])
        rows = connection.execute(
            """
            SELECT a.*
            FROM vep_annotations a
            JOIN target_rsids t USING (rsid)
            WHERE a.gene=?
            ORDER BY a.rsid, a.protein_id, a.transcript_id, a.protein_position_start,
                     a.hgvs_protein, a.consequence, a.ref_allele, a.alt_allele
            """,
            [gene],
        ).fetchall()
        columns = [item[0] for item in connection.description]
        annotations: list[dict[str, Any]] = []
        for raw in rows:
            row = {columns[index]: _json_safe(value) for index, value in enumerate(raw)}
            amino_acid, parsed_position = parse_hgvs_protein(row.get("hgvs_protein"))
            row.update({
                "variant_id": row.pop("rsid"),
                "amino_acid": amino_acid,
                "protein_position": row.get("protein_position_start") or parsed_position,
                "annotation_source": "Ensembl VEP",
                "annotation_version": "114",
                "genome_assembly": "GRCh38",
                "rsid_status": "resolved",
            })
            annotations.append(row)
        return annotations

    def build_gene(self, connection: duckdb.DuckDBPyConnection, gene: str) -> dict[str, Any]:
        contexts, input_rows = self._association_contexts(gene)
        all_annotations = self._annotations(connection, gene, contexts)
        vep_matched_rsids = {str(row["variant_id"]) for row in all_annotations}
        annotations = [
            row
            for row in all_annotations
            if isinstance(row.get("protein_position"), int) and row["protein_position"] > 0
        ]
        protein_position_rsids = {str(row["variant_id"]) for row in annotations}
        viewer_contexts = {
            rsid: contexts[rsid]
            for rsid in sorted(protein_position_rsids)
        }
        context_keys = sorted({
            (context["dataset_type"], context["phenotype_path_key"], context["source"])
            for rows in viewer_contexts.values()
            for context in rows
        })
        context_index = {key: index for index, key in enumerate(context_keys)}
        context_examples = {
            (context["dataset_type"], context["phenotype_path_key"], context["source"]): context
            for rows in viewer_contexts.values()
            for context in rows
        }
        context_definitions = []
        for key in context_keys:
            context = context_examples[key]
            context_definitions.append({
                "context_id": stable_id("protein-association-context", *key),
                "dataset_type": context["dataset_type"],
                "phenotype": context["phenotype"],
                "phenotype_path": context["phenotype_path"],
                "phenotype_path_key": context["phenotype_path_key"],
                "source": context["source"],
            })
        context_links = {
            rsid: [
                [
                    context_index[(
                        context["dataset_type"],
                        context["phenotype_path_key"],
                        context["source"],
                    )],
                    context["source_summary_count"],
                    context["minimum_reported_p_value"],
                ]
                for context in rows
            ]
            for rsid, rows in viewer_contexts.items()
        }
        association_rsids = set(contexts)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "gene": gene,
            "annotation": {
                "source": "Ensembl VEP",
                "version": "114",
                "genome_assembly": "GRCh38",
                "contract": ANNOTATION_CONTRACT,
                "selection": "all supplied positive protein-coordinate rows matching both association rsID and gene",
                "representative_transcript_selected": False,
            },
            "association_context_definitions": context_definitions,
            "association_context_links_by_variant": context_links,
            "annotations": annotations,
            "unresolved_variant_ids": sorted(association_rsids - vep_matched_rsids),
            "non_protein_variant_ids": sorted(vep_matched_rsids - protein_position_rsids),
            "counts": {
                "variant_index_rows": input_rows,
                "association_contexts": sum(len(rows) for rows in contexts.values()),
                "viewer_association_contexts": sum(len(rows) for rows in viewer_contexts.values()),
                "association_rsids": len(association_rsids),
                "vep_matched_rsids": len(vep_matched_rsids),
                "protein_position_rsids": len(protein_position_rsids),
                "unresolved_rsids": len(association_rsids - vep_matched_rsids),
                "non_protein_rsids": len(vep_matched_rsids - protein_position_rsids),
                "protein_consequence_annotations": len(annotations),
            },
            "limitations": [
                "Protein consequences are imported annotations; HeartBioPortal does not predict them at request time.",
                "One rsID can have multiple transcript or allele annotations and all supplied protein-coordinate rows remain separate.",
                "Association contexts and VEP annotations are linked by rsID and gene; no match is inferred from amino-acid position alone.",
                "Association rsIDs without a positive protein coordinate are not placed on the protein axis.",
            ],
        }
        if not annotations:
            return {
                "status": "no_protein_position_annotations",
                "path": None,
                "sha256": None,
                **payload["counts"],
            }

        path = self.genes_root / f"{gene}.json.gz"
        write_deterministic_json_gz(path, payload)
        return {
            "status": "published",
            "path": str(path),
            "sha256": sha256_file(path),
            **payload["counts"],
        }

    def run(
        self,
        genes: Iterable[str] | None = None,
        *,
        reset: bool = False,
        max_genes: int | None = None,
    ) -> dict[str, Any]:
        if reset:
            self.reset()
        self.genes_root.mkdir(parents=True, exist_ok=True)
        state = self._load_checkpoint()
        selected = sorted({str(gene).strip().upper() for gene in (genes or self.discover_genes()) if str(gene).strip()})
        if max_genes is not None:
            selected = selected[: max(0, int(max_genes))]
        checkpoint_config = {
            "schema_version": SCHEMA_VERSION,
            "variant_index_root": str(self.inputs.variant_index_root.resolve()),
            "vep_index_path": str(self.inputs.vep_index_path.resolve()),
            "vep_index_sha256": recorded_or_computed_sha256(self.inputs.vep_index_path),
            "selected_genes_sha256": hashlib.sha256(
                ("\n".join(selected) + "\n").encode("utf-8")
            ).hexdigest(),
        }
        if state.get("config") and state["config"] != checkpoint_config:
            raise ValueError("Checkpoint configuration differs from this run; use a new output root or --reset")
        state["config"] = checkpoint_config
        completed: dict[str, Any] = state.setdefault("completed_genes", {})
        started = time.monotonic()
        pending: list[tuple[int, str]] = []
        for index, gene in enumerate(selected, start=1):
            previous = completed.get(gene) or {}
            if previous.get("status") == "no_protein_position_annotations":
                continue
            if previous.get("path") and Path(previous["path"]).is_file():
                continue
            pending.append((index, gene))

        def record_result(index: int, gene: str, result: dict[str, Any]) -> None:
            completed[gene] = result
            if len(completed) % self.progress_interval == 0 or index == len(selected):
                self._write_checkpoint(state)
            if index == 1 or index % self.progress_interval == 0 or index == len(selected):
                self.logger.info(
                    "Protein consequence build progress: genes=%d/%d gene=%s rsids=%d annotations=%d elapsed=%.1fs",
                    index, len(selected), gene, result["association_rsids"],
                    result["protein_consequence_annotations"], time.monotonic() - started,
                )

        if self.workers == 1:
            connection = duckdb.connect(str(self.inputs.vep_index_path), read_only=True)
            try:
                for index, gene in pending:
                    gene_started = time.monotonic()
                    result = self.build_gene(connection, gene)
                    result["elapsed_seconds"] = round(time.monotonic() - gene_started, 3)
                    record_result(index, gene, result)
            finally:
                connection.close()
        elif pending:
            self.logger.info("Building gene payloads with %d worker processes", self.workers)
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=self.workers,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_initialize_build_worker,
                initargs=(self.inputs, self.progress_interval),
            ) as executor:
                for index, gene, result in executor.map(_build_gene_worker, pending, chunksize=1):
                    record_result(index, gene, result)

        self._write_checkpoint(state)
        artifact_genes = {
            gene: details
            for gene, details in sorted(completed.items())
            if details.get("path") and Path(details["path"]).is_file()
        }
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "built_at": utc_now(),
            "variant_index_root": str(self.inputs.variant_index_root.resolve()),
            "vep_index_path": str(self.inputs.vep_index_path.resolve()),
            "vep_index_sha256": recorded_or_computed_sha256(self.inputs.vep_index_path),
            "selected_genes_sha256": checkpoint_config["selected_genes_sha256"],
            "output_root": str(self.inputs.output_root.resolve()),
            "genes_requested": len(selected),
            "genes_completed": len([gene for gene in selected if gene in completed]),
            "artifact_genes": len(artifact_genes),
            "genes_without_protein_position_annotations": len([
                gene for gene in selected
                if (completed.get(gene) or {}).get("status") == "no_protein_position_annotations"
            ]),
            "counts": {
                key: sum(int(details.get(key) or 0) for details in artifact_genes.values())
                for key in (
                    "variant_index_rows", "association_contexts", "viewer_association_contexts",
                    "association_rsids", "vep_matched_rsids", "protein_position_rsids",
                    "unresolved_rsids", "non_protein_rsids", "protein_consequence_annotations",
                )
            },
            "gene_payloads": artifact_genes,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (self.inputs.output_root / "checksums.sha256").write_text(
            "".join(
                f"{details['sha256']}  {Path(details['path']).relative_to(self.inputs.output_root)}\n"
                for _, details in sorted(manifest["gene_payloads"].items())
            ),
            encoding="utf-8",
        )
        return manifest
