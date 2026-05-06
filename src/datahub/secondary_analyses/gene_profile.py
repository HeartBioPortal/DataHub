"""Gene-profile secondary-analysis assembly.

The module intentionally consumes local source snapshots instead of making live
API calls. DataHub builds should be reproducible, and the frontend/backend
should only see versioned artifacts.
"""

from __future__ import annotations

import csv
import gzip
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .artifacts import write_gene_payload_artifact, write_metadata
from .base import SecondaryAnalysisManifest, SecondaryArtifactRow


EMPTY_TEXT_VALUES = {"", "-", "na", "n/a", "none", "null"}
GENE_PROFILE_SCHEMA_VERSION = "gene_profile.v1"


@dataclass(frozen=True)
class HgncGeneRecord:
    hgnc_id: str
    symbol: str
    name: str
    status: str
    locus_group: str
    locus_type: str
    alias_symbols: tuple[str, ...]
    previous_symbols: tuple[str, ...]
    location: str
    entrez_gene_id: str
    ensembl_gene_id: str
    refseq_accessions: tuple[str, ...]
    uniprot_accessions: tuple[str, ...]
    gene_groups: tuple[str, ...]


@dataclass(frozen=True)
class GeneSummaryRecord:
    entrez_gene_id: str
    summary: str


@dataclass(frozen=True)
class UniProtRecord:
    accession: str
    gene_symbols: tuple[str, ...]
    protein_name: str
    reviewed: bool
    length_aa: int | None
    function_text: str
    subcellular_locations: tuple[str, ...]
    go_ids: tuple[str, ...]
    reactome_ids: tuple[str, ...]


@dataclass(frozen=True)
class GoAnnotationRecord:
    gene_symbol: str
    uniprot_accession: str
    go_id: str
    go_name: str
    aspect: str
    evidence_code: str
    reference: str
    assigned_by: str


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in EMPTY_TEXT_VALUES else text


def _split_multi(value: object) -> tuple[str, ...]:
    text = _clean_text(value)
    if not text:
        return ()
    parts = re.split(r"[|,]", text)
    return tuple(part.strip() for part in parts if _clean_text(part))


def _open_text(path: str | Path):
    file_path = Path(path)
    if file_path.suffix.lower() == ".gz":
        return gzip.open(file_path, "rt", encoding="utf-8", newline="")
    return file_path.open("r", encoding="utf-8", newline="")


def _first_sentence(text: str, *, max_chars: int = 260) -> str:
    cleaned = re.sub(r"\s+", " ", _clean_text(text))
    if not cleaned:
        return ""
    match = re.search(r"(?<=[.!?])\s+", cleaned)
    sentence = cleaned[: match.start()].strip() if match else cleaned
    if len(sentence) <= max_chars:
        return sentence
    truncated = sentence[: max_chars - 1].rsplit(" ", 1)[0].strip()
    return f"{truncated}." if truncated else sentence[:max_chars].strip()


def _load_json_or_jsonl(path: str | Path) -> Any:
    file_path = Path(path)
    with _open_text(file_path) as stream:
        if file_path.name.endswith(".jsonl") or file_path.name.endswith(".jsonl.gz"):
            return [json.loads(line) for line in stream if line.strip()]
        return json.load(stream)


def read_hgnc_records(path: str | Path) -> list[HgncGeneRecord]:
    """Read HGNC complete-set TSV or JSON records."""

    file_path = Path(path)
    records: Iterable[dict[str, Any]]
    if ".json" in file_path.suffixes or file_path.name.endswith(".json.gz"):
        payload = _load_json_or_jsonl(file_path)
        if isinstance(payload, dict):
            response = payload.get("response")
            docs = response.get("docs") if isinstance(response, dict) else None
            records = docs if isinstance(docs, list) else payload.get("docs", [])
        else:
            records = payload
    else:
        with _open_text(file_path) as stream:
            records = list(csv.DictReader(stream, delimiter="\t"))

    parsed: list[HgncGeneRecord] = []
    for row in records:
        if not isinstance(row, dict):
            continue
        hgnc_id = _clean_text(row.get("hgnc_id"))
        symbol = _clean_text(row.get("symbol")).upper()
        if not hgnc_id or not symbol:
            continue
        parsed.append(
            HgncGeneRecord(
                hgnc_id=hgnc_id,
                symbol=symbol,
                name=_clean_text(row.get("name")),
                status=_clean_text(row.get("status")),
                locus_group=_clean_text(row.get("locus_group")),
                locus_type=_clean_text(row.get("locus_type")),
                alias_symbols=tuple(item.upper() for item in _split_multi(row.get("alias_symbol"))),
                previous_symbols=tuple(item.upper() for item in _split_multi(row.get("prev_symbol"))),
                location=_clean_text(row.get("location")),
                entrez_gene_id=_clean_text(row.get("entrez_id") or row.get("entrez_gene_id")),
                ensembl_gene_id=_clean_text(row.get("ensembl_gene_id")),
                refseq_accessions=_split_multi(row.get("refseq_accession")),
                uniprot_accessions=_split_multi(row.get("uniprot_ids")),
                gene_groups=_split_multi(row.get("gene_group")),
            )
        )
    return parsed


def read_ncbi_gene_summaries(path: str | Path) -> dict[str, GeneSummaryRecord]:
    """Read NCBI gene_summary.gz-like TSV keyed by Entrez GeneID."""

    summaries: dict[str, GeneSummaryRecord] = {}
    with _open_text(path) as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        for row in reader:
            gene_id = _clean_text(
                row.get("GeneID")
                or row.get("#GeneID")
                or row.get("gene_id")
                or row.get("entrez_gene_id")
            )
            summary = _clean_text(row.get("Summary") or row.get("summary"))
            if gene_id and summary:
                summaries[gene_id] = GeneSummaryRecord(entrez_gene_id=gene_id, summary=summary)
    return summaries


def _uniprot_recommended_name(entry: dict[str, Any]) -> str:
    description = entry.get("proteinDescription")
    if not isinstance(description, dict):
        return _clean_text(entry.get("protein_name"))
    recommended = description.get("recommendedName")
    if isinstance(recommended, dict):
        full_name = recommended.get("fullName")
        if isinstance(full_name, dict):
            return _clean_text(full_name.get("value"))
    submission_names = description.get("submissionNames")
    if isinstance(submission_names, list) and submission_names:
        first = submission_names[0]
        if isinstance(first, dict) and isinstance(first.get("fullName"), dict):
            return _clean_text(first["fullName"].get("value"))
    return ""


def _uniprot_function_text(entry: dict[str, Any]) -> str:
    comments = entry.get("comments")
    if not isinstance(comments, list):
        return _clean_text(entry.get("function"))
    chunks: list[str] = []
    for comment in comments:
        if not isinstance(comment, dict) or comment.get("commentType") != "FUNCTION":
            continue
        for text_item in comment.get("texts") or []:
            if isinstance(text_item, dict):
                value = _clean_text(text_item.get("value"))
                if value:
                    chunks.append(value)
    return " ".join(chunks)


def _uniprot_gene_symbols(entry: dict[str, Any]) -> tuple[str, ...]:
    genes = entry.get("genes")
    symbols: list[str] = []
    if isinstance(genes, list):
        for gene in genes:
            if not isinstance(gene, dict):
                continue
            gene_name = gene.get("geneName")
            if isinstance(gene_name, dict):
                value = _clean_text(gene_name.get("value"))
                if value:
                    symbols.append(value.upper())
            for synonym in gene.get("synonyms") or []:
                if isinstance(synonym, dict):
                    value = _clean_text(synonym.get("value"))
                    if value:
                        symbols.append(value.upper())
    symbols.extend(item.upper() for item in _split_multi(entry.get("gene_names")))
    return tuple(dict.fromkeys(symbols))


def _uniprot_xrefs(entry: dict[str, Any], db_name: str) -> tuple[str, ...]:
    refs = entry.get("uniProtKBCrossReferences")
    values: list[str] = []
    if isinstance(refs, list):
        for ref in refs:
            if not isinstance(ref, dict) or ref.get("database") != db_name:
                continue
            value = _clean_text(ref.get("id"))
            if value:
                values.append(value)
    return tuple(dict.fromkeys(values))


def read_uniprot_records(path: str | Path) -> dict[str, UniProtRecord]:
    """Read UniProt REST search/stream JSON or JSONL records keyed by accession."""

    file_path = Path(path)
    if file_path.suffix.lower() in {".tsv", ".txt"} or file_path.name.endswith(".tsv.gz"):
        return read_uniprot_tsv_records(file_path)

    payload = _load_json_or_jsonl(path)
    if isinstance(payload, dict):
        raw_records = payload.get("results", [])
    else:
        raw_records = payload
    records: dict[str, UniProtRecord] = {}
    for entry in raw_records:
        if not isinstance(entry, dict):
            continue
        accession = _clean_text(entry.get("primaryAccession") or entry.get("accession"))
        if not accession:
            continue
        length = entry.get("sequence", {}).get("length") if isinstance(entry.get("sequence"), dict) else entry.get("length")
        try:
            length_aa = int(length) if length not in (None, "") else None
        except (TypeError, ValueError):
            length_aa = None
        records[accession] = UniProtRecord(
            accession=accession,
            gene_symbols=_uniprot_gene_symbols(entry),
            protein_name=_uniprot_recommended_name(entry) or _clean_text(entry.get("protein_name")),
            reviewed=bool(entry.get("reviewed") is True or entry.get("entryType") == "UniProtKB reviewed (Swiss-Prot)"),
            length_aa=length_aa,
            function_text=_uniprot_function_text(entry),
            subcellular_locations=(),
            go_ids=_uniprot_xrefs(entry, "GO"),
            reactome_ids=_uniprot_xrefs(entry, "Reactome"),
        )
    return records


def read_uniprot_tsv_records(path: str | Path) -> dict[str, UniProtRecord]:
    """Read UniProt stream/search TSV records keyed by accession."""

    records: dict[str, UniProtRecord] = {}
    with _open_text(path) as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        for row in reader:
            accession = _clean_text(
                row.get("Entry")
                or row.get("accession")
                or row.get("primaryAccession")
                or row.get("Accession")
            )
            if not accession:
                continue
            reviewed_text = _clean_text(row.get("Reviewed") or row.get("reviewed"))
            length_text = _clean_text(row.get("Length") or row.get("length"))
            try:
                length_aa = int(length_text) if length_text else None
            except ValueError:
                length_aa = None
            go_ids = tuple(
                item.strip()
                for item in re.split(r"[;,\s]+", _clean_text(row.get("Gene Ontology IDs") or row.get("go_id")))
                if item.strip().startswith("GO:")
            )
            reactome_ids = tuple(
                item.strip()
                for item in re.split(r"[;,\s]+", _clean_text(row.get("Reactome") or row.get("xref_reactome")))
                if item.strip()
            )
            records[accession] = UniProtRecord(
                accession=accession,
                gene_symbols=tuple(
                    dict.fromkeys(
                        item.upper()
                        for item in re.split(r"[;\s]+", _clean_text(row.get("Gene Names") or row.get("gene_names")))
                        if item.strip()
                    )
                ),
                protein_name=_clean_text(row.get("Protein names") or row.get("protein_name")),
                reviewed=reviewed_text.lower() in {"reviewed", "true", "yes", "uniprotkb reviewed (swiss-prot)"},
                length_aa=length_aa,
                function_text=_clean_text(row.get("Function [CC]") or row.get("cc_function") or row.get("Function")),
                subcellular_locations=(),
                go_ids=go_ids,
                reactome_ids=reactome_ids,
            )
    return records


def read_go_annotations(path: str | Path) -> list[GoAnnotationRecord]:
    """Read a compact GO annotation TSV used by the profile assembler."""

    annotations: list[GoAnnotationRecord] = []
    with _open_text(path) as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        for row in reader:
            gene_symbol = _clean_text(row.get("gene_symbol") or row.get("symbol")).upper()
            go_id = _clean_text(row.get("go_id") or row.get("GO_ID"))
            if not gene_symbol or not go_id:
                continue
            annotations.append(
                GoAnnotationRecord(
                    gene_symbol=gene_symbol,
                    uniprot_accession=_clean_text(row.get("uniprot_accession") or row.get("db_object_id")),
                    go_id=go_id,
                    go_name=_clean_text(row.get("go_name") or row.get("term_name")),
                    aspect=_clean_text(row.get("aspect")),
                    evidence_code=_clean_text(row.get("evidence_code") or row.get("evidence")),
                    reference=_clean_text(row.get("reference")),
                    assigned_by=_clean_text(row.get("assigned_by")),
                )
            )
    return annotations


def _read_protein_context(root: str | Path | None, gene: str) -> dict[str, Any] | None:
    if not root:
        return None
    base = Path(root)
    names = [f"{gene}.json.gz", f"{gene.upper()}.json.gz", f"{gene}.json", f"{gene.upper()}.json"]
    roots = [
        base,
        base / "genes",
        base / "final" / "protein_context" / "genes",
        base / "protein_context" / "genes",
    ]
    for root_path in roots:
        for name in names:
            candidate = root_path / name
            if candidate.exists():
                with _open_text(candidate) as stream:
                    payload = json.load(stream)
                return payload if isinstance(payload, dict) else None
    return None


def _select_uniprot(hgnc: HgncGeneRecord, uniprot_by_accession: dict[str, UniProtRecord]) -> tuple[UniProtRecord | None, bool]:
    candidates = [
        uniprot_by_accession[accession]
        for accession in hgnc.uniprot_accessions
        if accession in uniprot_by_accession
    ]
    if not candidates:
        candidates = [
            record
            for record in uniprot_by_accession.values()
            if hgnc.symbol in record.gene_symbols
        ]
    if not candidates:
        return None, False
    candidates = sorted(candidates, key=lambda item: (not item.reviewed, item.accession))
    return candidates[0], len(candidates) > 1


def _source_badges(*, summary: GeneSummaryRecord | None, uniprot: UniProtRecord | None, protein_context: dict[str, Any] | None, go_terms: list[GoAnnotationRecord]) -> list[str]:
    badges = ["HGNC"]
    if summary:
        badges.append("NCBI Gene")
    if uniprot:
        badges.append("UniProtKB")
    if protein_context:
        badges.append("HBP protein context")
    if go_terms:
        badges.append("GOA")
    return badges


def _chips(hgnc: HgncGeneRecord, uniprot: UniProtRecord | None, protein_context: dict[str, Any] | None, go_terms: list[GoAnnotationRecord]) -> list[str]:
    chips: list[str] = []
    locus = hgnc.locus_group or hgnc.locus_type
    if locus:
        chips.append(locus.replace("gene with protein product", "protein-coding"))
    if uniprot and uniprot.reviewed:
        chips.append("UniProt reviewed")
    if uniprot and uniprot.length_aa:
        chips.append(f"{uniprot.length_aa:,} aa")
    if protein_context and protein_context.get("isoforms"):
        chips.append("protein context")
    for annotation in go_terms[:3]:
        label = annotation.go_name or annotation.go_id
        if label and label not in chips:
            chips.append(label)
    return chips[:5]


def _go_display(go_terms: list[GoAnnotationRecord]) -> dict[str, list[dict[str, str]]]:
    aspect_map = {
        "F": "molecular_function",
        "P": "biological_process",
        "C": "cellular_component",
        "molecular_function": "molecular_function",
        "biological_process": "biological_process",
        "cellular_component": "cellular_component",
    }
    display = {"molecular_function": [], "biological_process": [], "cellular_component": []}
    seen: set[tuple[str, str]] = set()
    for term in go_terms:
        aspect = aspect_map.get(term.aspect, aspect_map.get(term.aspect.lower(), "biological_process"))
        key = (aspect, term.go_id)
        if key in seen or len(display[aspect]) >= 5:
            continue
        seen.add(key)
        display[aspect].append(
            {
                "go_id": term.go_id,
                "name": term.go_name,
                "evidence_code": term.evidence_code,
                "reference": term.reference,
                "assigned_by": term.assigned_by,
            }
        )
    return display


def build_gene_profile_payload(
    *,
    hgnc: HgncGeneRecord,
    ncbi_summary: GeneSummaryRecord | None = None,
    uniprot: UniProtRecord | None = None,
    multiple_uniprot_candidates: bool = False,
    protein_context: dict[str, Any] | None = None,
    go_terms: list[GoAnnotationRecord] | None = None,
) -> dict[str, Any]:
    go_terms = go_terms or []
    summary_sentence = _first_sentence(ncbi_summary.summary if ncbi_summary else "")
    if not summary_sentence and uniprot and uniprot.protein_name and uniprot.function_text:
        summary_sentence = f"{hgnc.symbol} encodes {uniprot.protein_name}, {_first_sentence(uniprot.function_text, max_chars=180).lower()}"
    if not summary_sentence:
        name = hgnc.name or hgnc.symbol
        summary_sentence = f"{hgnc.symbol} is a {hgnc.locus_type or hgnc.locus_group or 'gene'} known as {name}."

    protein_length = uniprot.length_aa if uniprot and uniprot.length_aa else None
    if protein_length is None and protein_context:
        for isoform in protein_context.get("isoforms") or []:
            if isinstance(isoform, dict) and isoform.get("canonical") and isoform.get("length_aa"):
                protein_length = isoform.get("length_aa")
                break

    quality_flags = {
        "symbol_conflict": False,
        "multiple_uniprot_candidates": multiple_uniprot_candidates,
        "ncbi_summary_missing": ncbi_summary is None,
        "unreviewed_uniprot_only": bool(uniprot and not uniprot.reviewed),
        "non_protein_coding": "protein" not in (hgnc.locus_group or hgnc.locus_type).lower(),
        "protein_context_missing": protein_context is None,
    }

    payload = {
        "schema_version": GENE_PROFILE_SCHEMA_VERSION,
        "identity": {
            "hgnc_id": hgnc.hgnc_id,
            "symbol": hgnc.symbol,
            "approved_name": hgnc.name,
            "status": hgnc.status,
            "locus_group": hgnc.locus_group,
            "locus_type": hgnc.locus_type,
            "aliases": list(hgnc.alias_symbols),
            "previous_symbols": list(hgnc.previous_symbols),
            "location": hgnc.location,
            "external_ids": {
                "ensembl_gene_id": hgnc.ensembl_gene_id,
                "entrez_gene_id": hgnc.entrez_gene_id,
                "refseq_accessions": list(hgnc.refseq_accessions),
                "uniprot_accessions": list(hgnc.uniprot_accessions),
            },
        },
        "summary": {
            "one_sentence": summary_sentence,
            "long_summary": ncbi_summary.summary if ncbi_summary else "",
            "summary_source": "NCBI Gene" if ncbi_summary else ("UniProtKB/HGNC template" if uniprot else "HGNC template"),
            "summary_source_id": hgnc.entrez_gene_id if ncbi_summary else "",
            "display_confidence": "high" if ncbi_summary else "medium",
        },
        "protein": {
            "has_protein_product": not quality_flags["non_protein_coding"],
            "recommended_name": uniprot.protein_name if uniprot else "",
            "primary_uniprot_accession": uniprot.accession if uniprot else "",
            "reviewed": bool(uniprot.reviewed) if uniprot else False,
            "length_aa": protein_length,
            "function_summary": _first_sentence(uniprot.function_text) if uniprot else "",
            "go_ids": list(uniprot.go_ids) if uniprot else [],
            "reactome_ids": list(uniprot.reactome_ids) if uniprot else [],
        },
        "ontology": {
            "go_terms_display": _go_display(go_terms),
            "go_terms_all_count": len(go_terms),
        },
        "display": {
            "chips": _chips(hgnc, uniprot, protein_context, go_terms),
            "source_badges": _source_badges(
                summary=ncbi_summary,
                uniprot=uniprot,
                protein_context=protein_context,
                go_terms=go_terms,
            ),
        },
        "quality_flags": quality_flags,
        "provenance": [
            {"source": "HGNC", "fields": ["identity"], "source_id": hgnc.hgnc_id},
        ],
    }
    if ncbi_summary:
        payload["provenance"].append({"source": "NCBI Gene", "fields": ["summary"], "source_id": hgnc.entrez_gene_id})
    if uniprot:
        payload["provenance"].append({"source": "UniProtKB", "fields": ["protein"], "source_id": uniprot.accession})
    if protein_context:
        payload["provenance"].append({"source": "HBP protein_context", "fields": ["protein"], "source_id": hgnc.symbol})
    if go_terms:
        payload["provenance"].append({"source": "GOA", "fields": ["ontology"], "source_id": hgnc.symbol})
    return payload


def card_from_gene_profile(payload: dict[str, Any]) -> dict[str, Any]:
    identity = payload.get("identity") or {}
    summary = payload.get("summary") or {}
    protein = payload.get("protein") or {}
    display = payload.get("display") or {}
    return {
        "hgnc_id": identity.get("hgnc_id", ""),
        "symbol": identity.get("symbol", ""),
        "approved_name": identity.get("approved_name", ""),
        "locus_type": identity.get("locus_type") or identity.get("locus_group", ""),
        "one_sentence": summary.get("one_sentence", ""),
        "protein_name": protein.get("recommended_name", ""),
        "primary_uniprot_accession": protein.get("primary_uniprot_accession", ""),
        "reviewed_uniprot": bool(protein.get("reviewed")),
        "length_aa": protein.get("length_aa"),
        "chips": display.get("chips") or [],
        "source_badges": display.get("source_badges") or [],
        "quality_flags": payload.get("quality_flags") or {},
    }


def _hgnc_file_id(hgnc_id: str) -> str:
    return hgnc_id.replace(":", "_")


def _write_json_index(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")


def _write_parquet_or_jsonl(path: Path, rows: list[dict[str, Any]]) -> str:
    fallback = path.with_suffix(".jsonl")
    _write_json_index(fallback, rows)
    try:
        import pandas as pd  # type: ignore

        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)
        return str(path)
    except Exception:
        return str(fallback)


def _symbol_map_rows(hgnc_records: list[HgncGeneRecord]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in hgnc_records:
        for symbol, kind in [(record.symbol, "approved")]:
            rows.append({"symbol": symbol, "hgnc_id": record.hgnc_id, "approved_symbol": record.symbol, "match_type": kind})
        for alias in record.alias_symbols:
            rows.append({"symbol": alias, "hgnc_id": record.hgnc_id, "approved_symbol": record.symbol, "match_type": "alias"})
        for previous in record.previous_symbols:
            rows.append({"symbol": previous, "hgnc_id": record.hgnc_id, "approved_symbol": record.symbol, "match_type": "previous"})
    return rows


def generate_gene_profile_artifacts(
    *,
    hgnc_path: str | Path,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    ncbi_gene_summary_path: str | Path | None = None,
    uniprot_path: str | Path | None = None,
    go_annotation_path: str | Path | None = None,
    protein_context_root: str | Path | None = None,
    include_genes: set[str] | None = None,
) -> list[SecondaryArtifactRow]:
    hgnc_records = read_hgnc_records(hgnc_path)
    summaries = read_ncbi_gene_summaries(ncbi_gene_summary_path) if ncbi_gene_summary_path else {}
    uniprot_records = read_uniprot_records(uniprot_path) if uniprot_path else {}
    go_annotations = read_go_annotations(go_annotation_path) if go_annotation_path else []
    go_by_symbol: dict[str, list[GoAnnotationRecord]] = {}
    for annotation in go_annotations:
        go_by_symbol.setdefault(annotation.gene_symbol, []).append(annotation)

    include = {gene.upper() for gene in include_genes} if include_genes else None
    rows: list[SecondaryArtifactRow] = []
    card_rows: list[dict[str, Any]] = []
    selected_hgnc_records: list[HgncGeneRecord] = []
    missing_summary_count = 0
    multiple_uniprot_count = 0

    for hgnc in hgnc_records:
        if include is not None and hgnc.symbol not in include and hgnc.hgnc_id.upper() not in include:
            continue
        selected_hgnc_records.append(hgnc)
        summary = summaries.get(hgnc.entrez_gene_id)
        uniprot, multiple_uniprot = _select_uniprot(hgnc, uniprot_records)
        protein_context = _read_protein_context(protein_context_root, hgnc.symbol)
        payload = build_gene_profile_payload(
            hgnc=hgnc,
            ncbi_summary=summary,
            uniprot=uniprot,
            multiple_uniprot_candidates=multiple_uniprot,
            protein_context=protein_context,
            go_terms=go_by_symbol.get(hgnc.symbol, []),
        )
        if summary is None:
            missing_summary_count += 1
        if multiple_uniprot:
            multiple_uniprot_count += 1
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        artifact_path = write_gene_payload_artifact(
            output_root=output_root,
            manifest=manifest,
            gene_id=_hgnc_file_id(hgnc.hgnc_id),
            payload_json=payload_json,
        )
        rows.append(
            SecondaryArtifactRow(
                gene_id=hgnc.hgnc_id,
                gene_id_normalized=hgnc.symbol,
                payload_json=payload_json,
                source_path=str(artifact_path),
            )
        )
        card_rows.append(card_from_gene_profile(payload))

    root = Path(output_root) / "final" / manifest.artifact_subdir
    index_path = _write_parquet_or_jsonl(root / "gene_profile.index.parquet", card_rows)
    symbol_map_path = _write_parquet_or_jsonl(root / "symbol_to_hgnc.parquet", _symbol_map_rows(selected_hgnc_records))
    write_metadata(
        output_root=output_root,
        manifest=manifest,
        filename="manifest.json",
        payload={
            "analysis_id": manifest.analysis_id,
            "version": manifest.version,
            "schema_version": GENE_PROFILE_SCHEMA_VERSION,
            "source_paths": {
                "hgnc": str(hgnc_path),
                "ncbi_gene_summary": str(ncbi_gene_summary_path or ""),
                "uniprot": str(uniprot_path or ""),
                "go_annotations": str(go_annotation_path or ""),
                "protein_context": str(protein_context_root or ""),
            },
            "outputs": {
                "index": index_path,
                "symbol_to_hgnc": symbol_map_path,
            },
            "coverage": {
                "hgnc_records": len(hgnc_records),
                "profile_count": len(rows),
                "ncbi_summary_missing": missing_summary_count,
                "multiple_uniprot_candidates": multiple_uniprot_count,
            },
        },
    )
    return rows
