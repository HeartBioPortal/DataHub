from __future__ import annotations

import gzip
import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from datahub.protein_consequence_rsid import (
    BuildInputs,
    ProteinConsequenceRsidBuilder,
    _configure_build_connection,
    parse_hgvs_protein,
)


def _write_variant_index(root: Path, dataset_type: str, gene: str, rows: list[dict]) -> None:
    path = root / dataset_type / f"{gene}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(rows, handle)


def _fixture(tmp_path: Path) -> tuple[Path, Path]:
    annotations = tmp_path / "annotations.csv"
    annotations.write_text(
        "rsid,gene,transcript,ENSP,protein_length,protein_pos_start,protein_pos_end,HGVSc,HGVSp,Consequence,BIOTYPE,MANE_SELECT,chromosome,position,ref,alt,variant_id,CAF_ref,CAF_alt\n"
        "rs1,PCSK9,ENST1,ENSP1,692,50,50,c.149A>G,p.Asp50Gly,missense_variant,protein_coding,YES,1,100,A,G,1:100:A:G,0.9,0.1\n"
        "rs1,PCSK9,ENST1,ENSP1,692,50,50,c.149A>G,p.Asp50Gly,missense_variant,protein_coding,YES,1,100,A,G,1:100:A:G,0.9,0.1\n"
        "rs1,PCSK9,ENST2,ENSP2,680,42,42,c.125A>G,p.Asp42Gly,missense_variant,protein_coding,,1,100,A,G,1:100:A:G,0.9,0.1\n"
        "rs2,PCSK9,ENST1,ENSP1,692,100,100,c.300C>T,p.Gly100=,synonymous_variant,protein_coding,YES,1,200,C,T,1:200:C:T,0.7,0.3\n"
        "rs3,PCSK9,ENST1,ENSP1,692,,,c.301+1C>T,,intron_variant,protein_coding,YES,1,201,C,T,1:201:C:T,0.8,0.2\n"
        "not-an-rsid,PCSK9,ENST1,ENSP1,692,101,101,c.303C>T,p.Gly101=,synonymous_variant,protein_coding,YES,1,201,C,T,1:201:C:T,0.7,0.3\n",
        encoding="utf-8",
    )
    index = tmp_path / "vep.duckdb"
    ProteinConsequenceRsidBuilder.create_vep_index(annotations, index)
    return annotations, index


def test_gene_discovery_intersects_approved_symbols(tmp_path: Path) -> None:
    variant_root = tmp_path / "variant_index"
    _write_variant_index(variant_root, "CVD", "PCSK9", [])
    _write_variant_index(variant_root, "TRAIT", "OLD_ALIAS", [])
    builder = ProteinConsequenceRsidBuilder(
        BuildInputs(variant_root, tmp_path / "unused.duckdb", tmp_path / "output")
    )

    assert builder.discover_genes() == ["OLD_ALIAS", "PCSK9"]
    assert builder.discover_genes(["PCSK9", "TTN"]) == ["PCSK9"]


def test_parse_hgvs_protein() -> None:
    assert parse_hgvs_protein("ENSP1:p.Asp50Gly") == ("D/G", 50)
    assert parse_hgvs_protein("p.Gly100=") == ("G/=", 100)
    assert parse_hgvs_protein("") == (None, None)


def test_build_connection_uses_bounded_duckdb_settings() -> None:
    class RecordingConnection:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement: str) -> None:
            self.statements.append(statement)

    connection = RecordingConnection()
    _configure_build_connection(connection)
    assert connection.statements == ["SET threads=1", "SET memory_limit='2GB'"]


def test_mixed_case_gene_artifact_is_resolved(tmp_path: Path) -> None:
    variant_root = tmp_path / 'variant_index'
    _write_variant_index(variant_root, 'CVD', 'C16orf46', [{
        'variant_id': 'rs1', 'phenotype': 'hypertension',
        'phenotype_path': ['hypertension'], 'source': 'million_veteran_program'
    }])
    builder = ProteinConsequenceRsidBuilder(BuildInputs(variant_root, tmp_path/'unused', tmp_path/'output'))
    assert builder._variant_index_path('CVD', 'C16ORF46').name == 'C16orf46.json.gz'
    contexts, count = builder._association_contexts('C16ORF46')
    assert count == 1
    assert set(contexts) == {'rs1'}


def test_index_retains_every_transcript_annotation(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    connection = duckdb.connect(str(index), read_only=True)
    try:
        assert connection.execute("SELECT count(*) FROM vep_annotations").fetchone()[0] == 5
        assert connection.execute(
            "SELECT count(*) FROM vep_annotations WHERE rsid='rs1'"
        ).fetchone()[0] == 3
        assert connection.execute(
            "SELECT count(DISTINCT annotation_id) FROM vep_annotations WHERE rsid='rs1'"
        ).fetchone()[0] == 3
        ordered = connection.execute(
            "SELECT gene, rsid, transcript_id FROM vep_annotations"
        ).fetchall()
        assert ordered == sorted(ordered)
    finally:
        connection.close()


def test_index_resume_rejects_changed_source_checksum(tmp_path: Path) -> None:
    annotations = tmp_path / "annotations.csv"
    annotations.write_text(
        "rsid,gene,transcript,ENSP,protein_length,protein_pos_start,protein_pos_end,HGVSc,HGVSp,Consequence,BIOTYPE,MANE_SELECT,chromosome,position,ref,alt,variant_id,CAF_ref,CAF_alt\n"
        "rs1,PCSK9,ENST1,ENSP1,692,50,50,c.149A>G,p.Asp50Gly,missense_variant,protein_coding,YES,1,100,A,G,1:100:A:G,0.9,0.1\n",
        encoding="utf-8",
    )
    index = tmp_path / "vep.duckdb"
    ProteinConsequenceRsidBuilder.create_vep_index(
        annotations, index, source_sha256="0" * 64
    )
    with pytest.raises(ValueError, match="source checksum differs"):
        ProteinConsequenceRsidBuilder.create_vep_index(
            annotations, index, source_sha256="1" * 64
        )


def test_gene_payload_preserves_rsids_contexts_and_annotations(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    variant_root = tmp_path / "variant_index"
    _write_variant_index(
        variant_root,
        "CVD",
        "PCSK9",
        [
            {
                "variant_id": "rs1",
                "gene_id": "PCSK9",
                "dataset_type": "CVD",
                "phenotype": "hypertension",
                "phenotype_path": ["vascular_diseases", "hypertensive_disorders", "hypertension"],
                "p_value": 0.02,
                "source": "million_veteran_program",
            },
            {
                "variant_id": "rs1",
                "gene_id": "PCSK9",
                "dataset_type": "CVD",
                "phenotype": "hypertension",
                "phenotype_path": ["vascular_diseases", "hypertensive_disorders", "hypertension"],
                "p_value": 0.01,
                "source": "legacy_cvd",
            },
            {
                "variant_id": "rs3",
                "gene_id": "PCSK9",
                "dataset_type": "CVD",
                "phenotype": "hypertension",
                "phenotype_path": ["vascular_diseases", "hypertensive_disorders", "hypertension"],
                "source": "legacy_cvd",
            },
            {
                "variant_id": "rs999",
                "gene_id": "PCSK9",
                "dataset_type": "CVD",
                "phenotype": "hypertension",
                "phenotype_path": ["vascular_diseases", "hypertensive_disorders", "hypertension"],
                "source": "million_veteran_program",
            },
        ],
    )
    _write_variant_index(
        variant_root,
        "TRAIT",
        "PCSK9",
        [{
            "variant_id": "rs2",
            "gene_id": "PCSK9",
            "dataset_type": "TRAIT",
            "phenotype": "ldl_cholesterol",
            "phenotype_path": ["lipid_traits", "ldl_cholesterol"],
            "p_value": 1e-8,
            "sources": ["legacy_trait"],
        }],
    )
    output = tmp_path / "output"
    builder = ProteinConsequenceRsidBuilder(
        BuildInputs(variant_root, index, output), progress_interval=1
    )
    manifest = builder.run(["PCSK9"])

    with gzip.open(output / "genes" / "PCSK9.json.gz", "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["counts"] == {
        "variant_index_rows": 5,
        "association_contexts": 5,
        "viewer_association_contexts": 3,
        "association_rsids": 4,
        "vep_matched_rsids": 3,
        "protein_position_rsids": 2,
        "unresolved_rsids": 1,
        "non_protein_rsids": 1,
        "protein_consequence_annotations": 4,
    }
    assert [row["variant_id"] for row in payload["annotations"]].count("rs1") == 3
    assert {row["transcript_id"] for row in payload["annotations"] if row["variant_id"] == "rs1"} == {"ENST1", "ENST2"}
    assert payload["unresolved_variant_ids"] == ["rs999"]
    definitions = payload["association_context_definitions"]
    rs1_sources = {definitions[link[0]]["source"] for link in payload["association_context_links_by_variant"]["rs1"]}
    assert rs1_sources == {"million_veteran_program", "legacy_cvd"}
    assert manifest["genes_completed"] == 1
    assert manifest["artifact_genes"] == 1
    assert manifest["worker_runtime"] == {
        "processes": 1,
        "duckdb_threads_per_process": 1,
        "duckdb_memory_limit_per_process": "2GB",
    }

    first_bytes = (output / "genes" / "PCSK9.json.gz").read_bytes()
    resumed = builder.run(["PCSK9"])
    assert resumed["genes_completed"] == 1
    assert (output / "genes" / "PCSK9.json.gz").read_bytes() == first_bytes

    rebuilt = builder.run(["PCSK9"], reset=True)
    assert rebuilt["genes_completed"] == 1
    assert (output / "genes" / "PCSK9.json.gz").read_bytes() == first_bytes

def test_gene_without_protein_coordinate_is_checkpointed_but_not_published(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    variant_root = tmp_path / "variant_index"
    _write_variant_index(
        variant_root,
        "CVD",
        "EMPTY",
        [{
            "variant_id": "rs999",
            "phenotype": "hypertension",
            "phenotype_path": ["vascular_diseases", "hypertension"],
            "sources": ["legacy_cvd"],
        }],
    )
    output_root = tmp_path / "output"
    builder = ProteinConsequenceRsidBuilder(
        BuildInputs(variant_root, index, output_root), workers=2
    )

    first = builder.run(["EMPTY"])
    second = builder.run(["EMPTY"])

    assert first["artifact_genes"] == 0
    assert first["genes_without_protein_position_annotations"] == 1
    assert second["genes_completed"] == 1
    assert not (output_root / "genes" / "EMPTY.json.gz").exists()
    checkpoint = json.loads((output_root / "build-checkpoint.json").read_text())
    assert checkpoint["completed_genes"]["EMPTY"]["association_scope_read"] is False
    assert checkpoint["completed_genes"]["EMPTY"]["association_rsids"] is None


def test_empty_explicit_gene_scope_does_not_build_all_genes(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    variant_root = tmp_path / "variant_index"
    _write_variant_index(variant_root, "CVD", "PCSK9", [{"variant_id": "rs1"}])
    builder = ProteinConsequenceRsidBuilder(BuildInputs(variant_root, index, tmp_path / "output"))
    manifest = builder.run([])
    assert manifest["genes_requested"] == 0
    assert manifest["artifact_genes"] == 0


def test_missing_protein_coordinates_skip_association_file_read(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    variant_root = tmp_path / "variant_index"
    path = variant_root / "CVD" / "EMPTY.json.gz"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"deliberately not a gzip file")
    builder = ProteinConsequenceRsidBuilder(BuildInputs(variant_root, index, tmp_path / "output"))
    manifest = builder.run(["EMPTY"])
    assert manifest["genes_without_protein_position_annotations"] == 1


def test_mixed_case_zero_count_checkpoint_is_retried(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    root = tmp_path / "variant_index"
    _write_variant_index(root, "CVD", "Pcsk9", [{
        "variant_id": "rs1", "phenotype": "hypertension",
        "phenotype_path": ["vascular", "hypertension"], "source": "million_veteran_program",
    }])
    output = tmp_path / "output"
    builder = ProteinConsequenceRsidBuilder(BuildInputs(root, index, output))
    builder.run(["PCSK9"])
    path = output / "build-checkpoint.json"
    checkpoint = json.loads(path.read_text())
    checkpoint["completed_genes"]["PCSK9"] = {
        "status": "no_protein_position_annotations", "association_rsids": 0, "path": None,
    }
    path.write_text(json.dumps(checkpoint))
    (output / "genes" / "PCSK9.json.gz").unlink()
    manifest = builder.run(["PCSK9"])
    assert manifest["artifact_genes"] == 1
    assert manifest["counts"]["association_rsids"] == 1


def test_ambiguous_mixed_case_names_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "variant_index"
    for gene in ["Pcsk9", "pcsk9"]:
        _write_variant_index(root, "CVD", gene, [])
    builder = ProteinConsequenceRsidBuilder(BuildInputs(root, tmp_path / "unused", tmp_path / "output"))
    with pytest.raises(ValueError, match="Ambiguous gene filename"):
        builder._variant_index_path("CVD", "PCSK9")
    with pytest.raises(ValueError, match="Ambiguous gene filename"):
        builder._variant_index_path("CVD", "PCSK9")


def test_profile_cli_intersection_and_empty_scope(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    root = tmp_path / "variant_index"
    _write_variant_index(root, "CVD", "PCSK9", [{"variant_id": "rs1"}])
    _write_variant_index(root, "CVD", "NOT_APPROVED", [{"variant_id": "rs1"}])
    for number, symbols in enumerate([["PCSK9", "NOT_INDEXED"], ["NOT_INDEXED"], []]):
        profile = tmp_path / f"profile{number}.jsonl"
        profile.write_text("".join(json.dumps({"symbol": symbol}) + "\n" for symbol in symbols))
        output = tmp_path / f"cli-output{number}"
        result = subprocess.run([
            sys.executable, "scripts/dataset_specific_scripts/unified/build_protein_consequence_rsid_index.py",
            "build", "--variant-index-root", str(root), "--vep-index", str(index),
            "--output-root", str(output), "--gene-profile-index", str(profile),
        ], text=True, capture_output=True, check=True)
        manifest = json.loads(result.stdout)
        assert manifest["genes_requested"] == (1 if number == 0 else 0)
        assert set(manifest["gene_payloads"]) == ({"PCSK9"} if number == 0 else set())


def test_projected_contexts_match_original_context_rules(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    root = tmp_path / "variant_index"
    common = {"variant_id": " RS1 ", "phenotype_path": [" vascular ", "hypertension"], "phenotype": "hypertension"}
    _write_variant_index(root, "CVD", "PCSK9", [
        {**common, "p_value": "bad", "sources": ["million_veteran_program", "legacy_cvd"]},
        {**common, "p_value": "0.02", "sources": ["million_veteran_program"]},
        {**common, "p_value": 0.001, "source": "legacy_cvd"},
        {"variant_id": "rs999", "phenotype_path": ["other"], "source": "legacy_cvd"},
        {"variant_id": "not-an-rsid", "p_value": 0.0},
    ])
    _write_variant_index(root, "TRAIT", "PCSK9", [
        {**common, "p_value": None, "source": "legacy_trait"},
        {"variant_id": "rs2", "phenotype_path": ["ldl"], "sources": []},
    ])
    builder = ProteinConsequenceRsidBuilder(BuildInputs(root, index, tmp_path / "output"))
    original, original_count = builder._association_contexts("PCSK9")
    with duckdb.connect(str(index), read_only=True) as connection:
        _configure_build_connection(connection)
        contexts, input_count, rsids, context_count = builder._projected_contexts(connection, "PCSK9", ["rs1", "rs2"])
        assert contexts == {rsid: original[rsid] for rsid in ["rs1", "rs2"]}
        assert input_count == original_count == 7
        assert rsids == set(original)
        assert context_count == sum(len(rows) for rows in original.values())
        assert connection.execute("SELECT count(*) FROM duckdb_tables() WHERE temporary").fetchone()[0] == 0


def test_parallel_and_serial_payloads_match(tmp_path: Path) -> None:
    _, index = _fixture(tmp_path)
    root = tmp_path / "variant_index"
    _write_variant_index(root, "CVD", "PCSK9", [{
        "variant_id": "rs1", "phenotype_path": ["hypertension"], "source": "million_veteran_program",
    }])
    for workers in [1, 2]:
        builder = ProteinConsequenceRsidBuilder(BuildInputs(root, index, tmp_path / f"output{workers}"), workers=workers)
        manifest = builder.run(["PCSK9"])
        assert manifest["worker_runtime"]["duckdb_memory_limit_per_process"] == "2GB"
    assert (tmp_path / "output1/genes/PCSK9.json.gz").read_bytes() == (tmp_path / "output2/genes/PCSK9.json.gz").read_bytes()
