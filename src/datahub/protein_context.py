"""Build protein-coordinate context artifacts for the splicing viewer."""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping, Protocol


logger = logging.getLogger(__name__)


class EnsemblProteinContextClient(Protocol):
    """Minimal Ensembl API surface used for protein-context generation."""

    def lookup_symbol(
        self,
        symbol: str,
        *,
        species: str = "homo_sapiens",
        expand: bool = False,
    ) -> dict[str, Any]:
        """Return expanded gene/transcript payload for a symbol."""

    def xrefs_id(
        self,
        identifier: str,
        *,
        external_db: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return cross references for an Ensembl identifier."""

    def xrefs_name(
        self,
        name: str,
        *,
        species: str = "homo_sapiens",
        external_db: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return Ensembl cross references matching a source name."""

    def overlap_translation(
        self,
        translation_id: str,
        *,
        feature: str = "protein_feature",
        species: str = "homo_sapiens",
        type_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return translation-coordinate features."""


class EbiProteinsFeatureClient(Protocol):
    """Minimal EBI Proteins API surface."""

    def features(self, accession: str) -> list[dict[str, Any]]:
        """Return UniProt feature payloads."""


class InterProFeatureClient(Protocol):
    """Minimal InterPro API surface."""

    def entries_for_uniprot(self, accession: str) -> list[dict[str, Any]]:
        """Return InterPro entries matching a UniProt accession."""


@dataclass(frozen=True)
class IsoformHint:
    """Isoform metadata inferred from an existing splicing-viewer row set."""

    identifier: str
    length_aa: int | None = None
    row_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ProteinContextReport:
    """Per-gene generation status."""

    gene: str
    isoforms_built: int = 0
    ensembl_gene_id: str = ""
    warnings: list[str] = field(default_factory=list)
    source_errors: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _strip_version(identifier: str) -> str:
    return _clean_text(identifier).split(".", 1)[0]


def _status_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _to_int(value: Any) -> int | None:
    if isinstance(value, Mapping):
        for key in ("value", "position", "start", "end"):
            parsed = _to_int(value.get(key))
            if parsed is not None:
                return parsed
        return None
    text = _clean_text(value)
    if not text or text in {"?", "-"}:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _range_from_payload(payload: Mapping[str, Any]) -> tuple[int | None, int | None]:
    start = _to_int(payload.get("start"))
    end = _to_int(payload.get("end"))
    if start is None:
        start = _to_int(payload.get("begin"))
    if end is None:
        end = _to_int(payload.get("stop"))
    if end is None:
        end = _to_int(payload.get("end_position"))

    location = payload.get("location")
    if isinstance(location, Mapping):
        if start is None:
            start = _to_int(location.get("start") or location.get("begin"))
        if end is None:
            end = _to_int(location.get("end") or location.get("stop"))
    if start is None or end is None:
        return None, None
    if start > end:
        start, end = end, start
    return start, end


def _feature_record(
    *,
    start: int,
    end: int,
    label: str,
    feature_type: str,
    source: str,
    accession: str = "",
    description: str = "",
    raw: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "start": int(start),
        "end": int(end),
        "label": label or feature_type or accession or "Feature",
        "type": feature_type or "feature",
        "source": source,
        "accession": accession,
        "description": description,
        "raw_type": _clean_text((raw or {}).get("type")),
    }


def _category_for_feature(feature: Mapping[str, Any], *, source: str) -> str:
    text = " ".join(
        _clean_text(feature.get(key)).lower()
        for key in ("type", "label", "description", "raw_type")
    )
    if source == "ensembl_translation_exon" or "translation_exon" in text or "exon" == text:
        return "exons"
    if "transmembrane" in text or "topological domain" in text or "intramembrane" in text:
        return "transmembrane"
    if "low complexity" in text or "compositional bias" in text or "compositionally biased" in text:
        return "low_complexity"
    if "disorder" in text or "disordered" in text:
        return "disorder"
    if (
        source == "interpro"
        or "domain" in text
        or "family" in text
        or "homologous superfamily" in text
        or "pfam" in text
        or "superfamily" in text
    ):
        return "domains"
    if "repeat" in text:
        return "repeats"
    if "site" in text or "binding" in text or "active" in text or "metal" in text:
        return "sites"
    if "region" in text or "motif" in text or "zinc finger" in text:
        return "regions"
    return "other_features"


def _append_feature(target: dict[str, list[dict[str, Any]]], feature: dict[str, Any]) -> None:
    category = _category_for_feature(feature, source=feature.get("source", ""))
    target.setdefault(category, []).append(feature)


def _normalize_ensembl_translation_exons(features: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, feature in enumerate(features, start=1):
        start, end = _range_from_payload(feature)
        if start is None or end is None:
            continue
        rank = feature.get("rank") or feature.get("exon_rank") or feature.get("exon_number") or index
        normalized.append(
            _feature_record(
                start=start,
                end=end,
                label=f"Exon {rank}",
                feature_type="translation_exon",
                source="ensembl_translation_exon",
                accession=_clean_text(feature.get("id") or feature.get("exon_id")),
                description=_clean_text(feature.get("description")),
                raw=feature,
            )
        )
    return normalized


def _normalize_ensembl_protein_features(features: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for feature in features:
        start, end = _range_from_payload(feature)
        if start is None or end is None:
            continue
        accession = _clean_text(feature.get("interpro_ac") or feature.get("id") or feature.get("hit_name"))
        feature_type = _clean_text(feature.get("type") or feature.get("analysis"))
        label = _clean_text(feature.get("description") or feature.get("hit_name") or accession or feature_type)
        normalized.append(
            _feature_record(
                start=start,
                end=end,
                label=label,
                feature_type=feature_type,
                source="ensembl_protein_feature",
                accession=accession,
                description=_clean_text(feature.get("description")),
                raw=feature,
            )
        )
    return normalized


def _normalize_ebi_protein_features(features: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for feature in features:
        start, end = _range_from_payload(feature)
        if start is None or end is None:
            continue
        feature_type = _clean_text(feature.get("type") or feature.get("category"))
        accession = _clean_text(feature.get("accession") or feature.get("id"))
        label = _clean_text(
            feature.get("description")
            or feature.get("alternativeSequence")
            or feature.get("ligand")
            or accession
            or feature_type
        )
        normalized.append(
            _feature_record(
                start=start,
                end=end,
                label=label,
                feature_type=feature_type,
                source="ebi_proteins",
                accession=accession,
                description=_clean_text(feature.get("description")),
                raw=feature,
            )
        )
    return normalized


def _iter_interpro_fragments(payload: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        if "start" in payload and "end" in payload:
            yield payload
        for key in (
            "fragments",
            "entry_protein_locations",
            "locations",
            "proteins",
            "matches",
            "representative",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                for item in value:
                    yield from _iter_interpro_fragments(item)
            elif isinstance(value, Mapping):
                yield from _iter_interpro_fragments(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_interpro_fragments(item)


def _normalize_interpro_entries(entries: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, int, int]] = set()
    for entry in entries:
        metadata = entry.get("metadata") if isinstance(entry.get("metadata"), Mapping) else {}
        accession = _clean_text(metadata.get("accession") or entry.get("accession"))
        entry_type = _clean_text(metadata.get("type") or entry.get("type") or "InterPro")
        label = _clean_text(metadata.get("name") or metadata.get("short_name") or accession or entry_type)
        description = _clean_text(metadata.get("description"))
        for fragment in _iter_interpro_fragments(entry):
            start, end = _range_from_payload(fragment)
            if start is None or end is None:
                continue
            key = (accession, start, end)
            if key in seen:
                continue
            seen.add(key)
            normalized.append(
                _feature_record(
                    start=start,
                    end=end,
                    label=label,
                    feature_type=entry_type,
                    source="interpro",
                    accession=accession,
                    description=description,
                    raw=fragment,
                )
            )
    return normalized


def _xrefs_by_kind(xrefs: Iterable[Mapping[str, Any]]) -> dict[str, list[str]]:
    refseq: set[str] = set()
    uniprot: set[str] = set()
    for xref in xrefs:
        dbname = _clean_text(
            xref.get("dbname")
            or xref.get("db_display_name")
            or xref.get("db_name")
            or xref.get("database")
        ).lower()
        value = _clean_text(
            xref.get("primary_id")
            or xref.get("display_id")
            or xref.get("id")
            or xref.get("dbname")
        )
        if not value:
            continue
        if "refseq" in dbname or value.startswith(("NM_", "NP_", "XM_", "XP_")):
            refseq.add(value)
        if "uniprot" in dbname or re.match(r"^[OPQ][0-9][A-Z0-9]{3}[0-9](-\d+)?$", value):
            uniprot.add(value)
    return {
        "refseq": sorted(refseq),
        "uniprot": sorted(uniprot),
    }


def _translation_payload(transcript: Mapping[str, Any]) -> Mapping[str, Any]:
    translation = transcript.get("Translation")
    if isinstance(translation, list):
        translation = translation[0] if translation else {}
    return translation if isinstance(translation, Mapping) else {}


def _protein_length(transcript: Mapping[str, Any], hints_by_identifier: Mapping[str, IsoformHint]) -> int | None:
    translation = _translation_payload(transcript)
    for value in (
        translation.get("length"),
        translation.get("seq_length"),
        transcript.get("protein_length"),
        transcript.get("translation_length"),
    ):
        parsed = _to_int(value)
        if parsed:
            return parsed
    for identifier in (
        _clean_text(transcript.get("id")),
        _clean_text(translation.get("id")),
        _clean_text(transcript.get("display_name")),
    ):
        hint = hints_by_identifier.get(identifier) or hints_by_identifier.get(_strip_version(identifier))
        if hint and hint.length_aa:
            return hint.length_aa
    return None


def _protein_coding_transcripts(gene_payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    transcripts = gene_payload.get("Transcript")
    if not isinstance(transcripts, list):
        return []
    selected: list[Mapping[str, Any]] = []
    for transcript in transcripts:
        if not isinstance(transcript, Mapping):
            continue
        if not _translation_payload(transcript):
            continue
        biotype = _clean_text(transcript.get("biotype")).lower()
        if biotype and biotype != "protein_coding":
            continue
        selected.append(transcript)
    return selected


def _hint_lookup(hints: Iterable[IsoformHint]) -> dict[str, IsoformHint]:
    lookup: dict[str, IsoformHint] = {}
    for hint in hints:
        identifier = _clean_text(hint.identifier)
        if not identifier:
            continue
        lookup[identifier] = hint
        lookup[_strip_version(identifier)] = hint
    return lookup


def _map_refseq_hints_to_transcripts(
    hints: Iterable[IsoformHint],
    *,
    ensembl_client: EnsemblProteinContextClient,
    species: str,
    report: ProteinContextReport,
) -> dict[str, set[str]]:
    mapped: dict[str, set[str]] = {}
    for hint in hints:
        identifier = _clean_text(hint.identifier)
        if not identifier or not identifier.startswith(("NM_", "XM_", "NP_", "XP_")):
            continue
        candidates = [identifier]
        stripped = _strip_version(identifier)
        if stripped != identifier:
            candidates.append(stripped)
        for candidate in candidates:
            try:
                xrefs = ensembl_client.xrefs_name(candidate, species=species)
            except Exception as exc:  # noqa: BLE001 - source failures are recorded per gene.
                report.source_errors[f"ensembl_xrefs_name:{candidate}"] = _status_error(exc)
                continue
            for xref in xrefs:
                target = _clean_text(xref.get("id") or xref.get("primary_id"))
                if target.startswith(("ENST", "ENSP")):
                    mapped.setdefault(identifier, set()).add(_strip_version(target))
    return mapped


def _select_transcripts(
    transcripts: list[Mapping[str, Any]],
    *,
    hints: Iterable[IsoformHint],
    hint_transcript_map: Mapping[str, set[str]],
    max_isoforms: int,
) -> list[Mapping[str, Any]]:
    hint_ids = {_strip_version(hint.identifier) for hint in hints if _clean_text(hint.identifier)}
    mapped_ids = {item for values in hint_transcript_map.values() for item in values}
    selected: list[Mapping[str, Any]] = []

    def should_include(transcript: Mapping[str, Any]) -> bool:
        transcript_id = _strip_version(_clean_text(transcript.get("id")))
        translation_id = _strip_version(_clean_text(_translation_payload(transcript).get("id")))
        display_name = _strip_version(_clean_text(transcript.get("display_name")))
        return (
            transcript_id in mapped_ids
            or translation_id in mapped_ids
            or transcript_id in hint_ids
            or translation_id in hint_ids
            or display_name in hint_ids
            or bool(transcript.get("is_canonical"))
            or bool(transcript.get("gencode_primary"))
        )

    for transcript in transcripts:
        if should_include(transcript):
            selected.append(transcript)

    if not selected:
        selected = transcripts[:max_isoforms]

    selected.sort(
        key=lambda transcript: (
            not bool(transcript.get("is_canonical")),
            not bool(transcript.get("gencode_primary")),
            _clean_text(transcript.get("id")),
        )
    )
    return selected[: max(int(max_isoforms), 1)]


def _merge_feature_groups(features: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {
        "exons": [],
        "domains": [],
        "transmembrane": [],
        "low_complexity": [],
        "disorder": [],
        "repeats": [],
        "sites": [],
        "regions": [],
        "other_features": [],
    }
    seen: set[tuple[str, int, int, str]] = set()
    for feature in features:
        key = (
            _clean_text(feature.get("source")),
            int(feature.get("start") or 0),
            int(feature.get("end") or 0),
            _clean_text(feature.get("accession") or feature.get("label")),
        )
        if key in seen:
            continue
        seen.add(key)
        _append_feature(groups, feature)
    for values in groups.values():
        values.sort(key=lambda item: (int(item.get("start") or 0), int(item.get("end") or 0), item.get("label", "")))
    return groups


def _build_isoform_context(
    transcript: Mapping[str, Any],
    *,
    ensembl_client: EnsemblProteinContextClient,
    proteins_client: EbiProteinsFeatureClient | None,
    interpro_client: InterProFeatureClient | None,
    hints_by_identifier: Mapping[str, IsoformHint],
    species: str,
) -> dict[str, Any]:
    transcript_id = _clean_text(transcript.get("id"))
    translation = _translation_payload(transcript)
    translation_id = _clean_text(translation.get("id"))
    source_status: dict[str, str] = {}
    transcript_xrefs: dict[str, list[str]] = {"refseq": [], "uniprot": []}
    protein_xrefs: dict[str, list[str]] = {"refseq": [], "uniprot": []}

    try:
        transcript_xrefs = _xrefs_by_kind(ensembl_client.xrefs_id(transcript_id)) if transcript_id else transcript_xrefs
        source_status["ensembl_transcript_xrefs"] = "ok"
    except Exception as exc:  # noqa: BLE001
        source_status["ensembl_transcript_xrefs"] = _status_error(exc)

    try:
        protein_xrefs = _xrefs_by_kind(ensembl_client.xrefs_id(translation_id)) if translation_id else protein_xrefs
        source_status["ensembl_protein_xrefs"] = "ok"
    except Exception as exc:  # noqa: BLE001
        source_status["ensembl_protein_xrefs"] = _status_error(exc)

    refseq_ids = sorted({*transcript_xrefs["refseq"], *protein_xrefs["refseq"]})
    uniprot_accessions = sorted({*transcript_xrefs["uniprot"], *protein_xrefs["uniprot"]})
    features: list[dict[str, Any]] = []

    if translation_id:
        try:
            features.extend(
                _normalize_ensembl_translation_exons(
                    ensembl_client.overlap_translation(
                        translation_id,
                        feature="translation_exon",
                        species=species,
                    )
                )
            )
            source_status["ensembl_translation_exon"] = "ok"
        except Exception as exc:  # noqa: BLE001
            source_status["ensembl_translation_exon"] = _status_error(exc)

        try:
            features.extend(
                _normalize_ensembl_protein_features(
                    ensembl_client.overlap_translation(
                        translation_id,
                        feature="protein_feature",
                        species=species,
                    )
                )
            )
            source_status["ensembl_protein_feature"] = "ok"
        except Exception as exc:  # noqa: BLE001
            source_status["ensembl_protein_feature"] = _status_error(exc)
    else:
        source_status["ensembl_translation"] = "missing_translation_id"

    accession = uniprot_accessions[0] if uniprot_accessions else ""
    if proteins_client is None:
        source_status["ebi_proteins"] = "skipped_no_client"
    elif not accession:
        source_status["ebi_proteins"] = "skipped_no_uniprot_accession"
    else:
        try:
            features.extend(_normalize_ebi_protein_features(proteins_client.features(accession)))
            source_status["ebi_proteins"] = "ok"
        except Exception as exc:  # noqa: BLE001
            source_status["ebi_proteins"] = _status_error(exc)

    if interpro_client is None:
        source_status["interpro"] = "skipped_no_client"
    elif not accession:
        source_status["interpro"] = "skipped_no_uniprot_accession"
    else:
        try:
            features.extend(_normalize_interpro_entries(interpro_client.entries_for_uniprot(accession)))
            source_status["interpro"] = "ok"
        except Exception as exc:  # noqa: BLE001
            source_status["interpro"] = _status_error(exc)

    feature_groups = _merge_feature_groups(features)
    return {
        "transcript_id": transcript_id,
        "protein_id": translation_id,
        "display_name": _clean_text(transcript.get("display_name") or transcript_id),
        "refseq_ids": refseq_ids,
        "uniprot_accessions": uniprot_accessions,
        "canonical": bool(transcript.get("is_canonical")),
        "gencode_primary": bool(transcript.get("gencode_primary")),
        "biotype": _clean_text(transcript.get("biotype")),
        "strand": transcript.get("strand"),
        "length_aa": _protein_length(transcript, hints_by_identifier),
        "genomic_start": _to_int(transcript.get("start")),
        "genomic_end": _to_int(transcript.get("end")),
        "source_status": source_status,
        **feature_groups,
    }


def build_protein_context_payload(
    gene: str,
    *,
    ensembl_client: EnsemblProteinContextClient,
    proteins_client: EbiProteinsFeatureClient | None = None,
    interpro_client: InterProFeatureClient | None = None,
    isoform_hints: Iterable[IsoformHint] | None = None,
    species: str = "homo_sapiens",
    max_isoforms: int = 12,
) -> tuple[dict[str, Any], ProteinContextReport]:
    """Build one normalized protein-context payload for a gene."""

    gene_symbol = _clean_text(gene).upper()
    hints = list(isoform_hints or [])
    hints_by_identifier = _hint_lookup(hints)
    report = ProteinContextReport(gene=gene_symbol)

    try:
        gene_payload = ensembl_client.lookup_symbol(gene_symbol, species=species, expand=True)
        report.ensembl_gene_id = _clean_text(gene_payload.get("id"))
    except Exception as exc:  # noqa: BLE001
        report.source_errors["ensembl_lookup_symbol"] = _status_error(exc)
        payload = {
            "gene": gene_symbol,
            "species": species,
            "isoforms": [],
            "isoform_hints": [hint.to_dict() for hint in hints],
            "source_status": {"ensembl_lookup_symbol": report.source_errors["ensembl_lookup_symbol"]},
        }
        return payload, report

    transcripts = _protein_coding_transcripts(gene_payload)
    hint_transcript_map = _map_refseq_hints_to_transcripts(
        hints,
        ensembl_client=ensembl_client,
        species=species,
        report=report,
    )
    selected_transcripts = _select_transcripts(
        transcripts,
        hints=hints,
        hint_transcript_map=hint_transcript_map,
        max_isoforms=max_isoforms,
    )

    isoforms = [
        _build_isoform_context(
            transcript,
            ensembl_client=ensembl_client,
            proteins_client=proteins_client,
            interpro_client=interpro_client,
            hints_by_identifier=hints_by_identifier,
            species=species,
        )
        for transcript in selected_transcripts
    ]
    report.isoforms_built = len(isoforms)
    if not isoforms:
        report.warnings.append("No protein-coding transcripts with translation IDs were selected")

    payload = {
        "gene": gene_symbol,
        "ensembl_gene_id": report.ensembl_gene_id,
        "species": species,
        "isoforms": isoforms,
        "isoform_hints": [hint.to_dict() for hint in hints],
        "hint_transcript_map": {
            key: sorted(values) for key, values in sorted(hint_transcript_map.items())
        },
        "source_status": {
            "ensembl_lookup_symbol": "ok",
            **report.source_errors,
        },
    }
    return payload, report
