from __future__ import annotations

import gzip
import json
from pathlib import Path

import duckdb
import pytest

from datahub.protein_consequence_rsid import (
    BuildInputs,
    ProteinConsequenceRsidBuilder,
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


def test_parse_hgvs_protein() -> None:
    assert parse_hgvs_protein("ENSP1:p.Asp50Gly") == ("D/G", 50)
    assert parse_hgvs_protein("p.Gly100=") == ("G/=", 100)
    assert parse_hgvs_protein("") == (None, None)


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
