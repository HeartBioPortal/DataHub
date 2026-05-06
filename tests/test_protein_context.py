import gzip
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.protein_context import IsoformHint, build_protein_context_payload
from datahub.secondary_analyses.base import SecondaryAnalysisManifest
from datahub.secondary_analyses.protein_context import (
    collect_association_db_genes,
    collect_variant_viewer_genes,
    generate_protein_context_artifacts,
    read_variant_viewer_isoform_hints,
)
from datahub.secondary_analyses.serving import apply_secondary_analysis_artifacts

try:
    import duckdb
except ImportError:  # pragma: no cover - test environment dependent
    duckdb = None


class _FakeEnsemblProteinClient:
    def __init__(self):
        self.calls = []

    def lookup_symbol(self, symbol, *, species="homo_sapiens", expand=False):
        self.calls.append(("lookup_symbol", symbol, species, expand))
        return {
            "id": "ENSGANK2",
            "Transcript": [
                {
                    "id": "ENSTANK2",
                    "display_name": "ANK2-202",
                    "biotype": "protein_coding",
                    "is_canonical": 1,
                    "strand": -1,
                    "Translation": {"id": "ENSPANK2", "length": 3957},
                },
                {
                    "id": "ENSTANK2ALT",
                    "display_name": "ANK2-205",
                    "biotype": "protein_coding",
                    "Translation": {"id": "ENSPANK2ALT", "length": 1200},
                },
            ],
        }

    def xrefs_id(self, identifier, *, external_db=None):
        self.calls.append(("xrefs_id", identifier, external_db))
        if identifier == "ENSTANK2":
            return [{"dbname": "RefSeq_mRNA", "primary_id": "NM_001148.6"}]
        if identifier == "ENSPANK2":
            return [{"dbname": "UniProtKB/Swiss-Prot", "primary_id": "Q01484"}]
        return []

    def xrefs_name(self, name, *, species="homo_sapiens", external_db=None):
        self.calls.append(("xrefs_name", name, species, external_db))
        if name in {"NM_001148.6", "NM_001148"}:
            return [{"id": "ENSTANK2"}]
        return []

    def overlap_translation(self, translation_id, *, feature="protein_feature", species="homo_sapiens", type_filter=None):
        self.calls.append(("overlap_translation", translation_id, feature, species, type_filter))
        if translation_id != "ENSPANK2":
            return []
        if feature == "translation_exon":
            return [
                {"id": "ENSE1", "start": 1, "end": 80, "rank": 1},
                {"id": "ENSE2", "start": 81, "end": 140, "rank": 2},
            ]
        return [
            {"type": "Domain", "start": 900, "end": 1200, "description": "Ankyrin repeat region"},
            {"type": "Transmembrane", "start": 2100, "end": 2125, "description": "Helical"},
        ]


class _FakeProteinsClient:
    def features(self, accession):
        assert accession == "Q01484"
        return [
            {"type": "LOW COMPLEXITY", "begin": "300", "end": "340", "description": "Low complexity"},
        ]


class _FakeInterProClient:
    def entries_for_uniprot(self, accession):
        assert accession == "Q01484"
        return [
            {
                "metadata": {"accession": "IPR000001", "name": "ANK repeat", "type": "domain"},
                "proteins": [
                    {
                        "entry_protein_locations": [
                            {"fragments": [{"start": 950, "end": 1010}]}
                        ]
                    }
                ],
            }
        ]


def test_build_protein_context_payload_merges_sources() -> None:
    payload, report = build_protein_context_payload(
        "ANK2",
        ensembl_client=_FakeEnsemblProteinClient(),
        proteins_client=_FakeProteinsClient(),
        interpro_client=_FakeInterProClient(),
        isoform_hints=[IsoformHint(identifier="NM_001148.6", length_aa=3957, row_count=10)],
    )

    assert report.isoforms_built == 1
    isoform = payload["isoforms"][0]
    assert isoform["transcript_id"] == "ENSTANK2"
    assert isoform["protein_id"] == "ENSPANK2"
    assert isoform["refseq_ids"] == ["NM_001148.6"]
    assert isoform["uniprot_accessions"] == ["Q01484"]
    assert isoform["length_aa"] == 3957
    assert [exon["label"] for exon in isoform["exons"]] == ["Exon 1", "Exon 2"]
    assert {domain["source"] for domain in isoform["domains"]} == {
        "ensembl_protein_feature",
        "interpro",
    }
    assert isoform["transmembrane"][0]["start"] == 2100
    assert isoform["low_complexity"][0]["source"] == "ebi_proteins"


def test_variant_viewer_hints_and_secondary_generation(tmp_path: Path) -> None:
    variant_root = tmp_path / "variant_viewer"
    overall = variant_root / "overall"
    overall.mkdir(parents=True)
    (overall / "ANK2.csv").write_text(
        "protein_id,max,value,mutation\n"
        "NM_001148.6,3957,100,missense\n"
        "NM_001148.6,3957,101,missense\n"
    )

    hints = read_variant_viewer_isoform_hints(variant_viewer_root=variant_root, gene="ANK2")
    assert hints == [IsoformHint(identifier="NM_001148.6", length_aa=3957, row_count=2)]

    manifest = SecondaryAnalysisManifest(
        analysis_id="protein_context",
        version=1,
        mode="derived",
        description="test",
        artifact_subdir="protein_context",
    )
    rows = generate_protein_context_artifacts(
        output_root=tmp_path / "secondary",
        manifest=manifest,
        variant_viewer_root=variant_root,
        ensembl_client=_FakeEnsemblProteinClient(),
        proteins_client=_FakeProteinsClient(),
        interpro_client=_FakeInterProClient(),
    )

    assert len(rows) == 1
    artifact = tmp_path / "secondary" / "final" / "protein_context" / "genes" / "ANK2.json.gz"
    with gzip.open(artifact, "rt", encoding="utf-8") as stream:
        payload = json.loads(stream.read())
    assert payload["gene"] == "ANK2"
    assert payload["isoforms"][0]["domains"]


def test_variant_viewer_gene_discovery_supports_compressed_tables(tmp_path: Path) -> None:
    variant_root = tmp_path / "variant_viewer"
    overall = variant_root / "overall"
    overall.mkdir(parents=True)
    with zipfile.ZipFile(overall / "TTN.csv.zip", "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("TTN.csv", "protein_id,max,value,mutation\nNM_001267550.2,35991,1,missense\n")
    with gzip.open(overall / "ANK2.tsv.gz", "wt", encoding="utf-8") as stream:
        stream.write("protein_id\tmax\tvalue\tmutation\nNM_001148.6\t3957\t100\tmissense\n")

    assert collect_variant_viewer_genes(variant_root) == {"ANK2", "TTN"}
    hints = read_variant_viewer_isoform_hints(variant_viewer_root=variant_root, gene="ANK2")
    assert hints == [IsoformHint(identifier="NM_001148.6", length_aa=3957, row_count=1)]


def test_protein_context_falls_back_to_association_db_genes(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    db_path = tmp_path / "association.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE mvp_association_points (gene_id VARCHAR)")
        con.execute("INSERT INTO mvp_association_points VALUES ('ANK2'), ('12345'), ('TTN')")
    finally:
        con.close()

    assert collect_association_db_genes(db_path) == {"ANK2", "TTN"}


def test_protein_context_zero_gene_source_fails_loudly(tmp_path: Path) -> None:
    import pytest

    manifest = SecondaryAnalysisManifest(
        analysis_id="protein_context",
        version=1,
        mode="derived",
        description="test",
        artifact_subdir="protein_context",
    )

    with pytest.raises(ValueError, match="zero candidate genes"):
        generate_protein_context_artifacts(
            output_root=tmp_path / "secondary",
            manifest=manifest,
            variant_viewer_root=tmp_path / "missing_variant_viewer",
            ensembl_client=_FakeEnsemblProteinClient(),
            proteins_client=_FakeProteinsClient(),
            interpro_client=_FakeInterProClient(),
        )


def test_protein_context_secondary_apply_updates_catalog(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    manifest = SecondaryAnalysisManifest(
        analysis_id="protein_context",
        version=1,
        mode="derived",
        description="test",
        artifact_subdir="protein_context",
    )
    secondary_root = tmp_path / "secondary"
    artifact = secondary_root / "final" / "protein_context" / "genes" / "ANK2.json.gz"
    artifact.parent.mkdir(parents=True)
    with gzip.open(artifact, "wt", encoding="utf-8") as stream:
        stream.write(json.dumps({"gene": "ANK2", "isoforms": []}))

    db_path = tmp_path / "serving.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
CREATE TABLE association_gene_payloads (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
        )
        con.execute(
            """
CREATE TABLE overall_gene_payloads (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
        )
        con.execute(
            """
CREATE TABLE gene_catalog (
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    has_cvd BOOLEAN,
    has_trait BOOLEAN,
    has_cvd_association BOOLEAN,
    has_trait_association BOOLEAN,
    has_cvd_overall BOOLEAN,
    has_trait_overall BOOLEAN,
    has_expression BOOLEAN,
    has_sga BOOLEAN
)
"""
        )
        inserted = apply_secondary_analysis_artifacts(
            con,
            input_root=secondary_root,
            manifest=manifest,
        )
        assert inserted == 1
        row = con.execute(
            """
SELECT has_protein_context
FROM gene_catalog
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert row == (True,)
    finally:
        con.close()
