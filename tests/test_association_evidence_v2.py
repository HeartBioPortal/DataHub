from __future__ import annotations

import csv
import gzip
import importlib.util
import json
import sys
from pathlib import Path

import duckdb

from datahub.association_evidence_v2.builder import EvidenceV2Builder
from datahub.association_evidence_v2.normalization import variation_type_from_alleles
from datahub.association_evidence_v2.serving import AssociationEvidenceV2ServingBuilder


HEADER = [
    "MarkerID",
    "pval",
    "Phenotype",
    "Study",
    "PMID",
    "StudyGenomeBuild",
    "dbsnp.rsid",
    "dbsnp.dbsnp_build",
    "dbsnp.alleles.allele",
    "dbsnp.chrom",
    "dbsnp.hg19.start",
    "dbsnp.hg19.end",
    "dbsnp.vartype",
    "gnomad_genome.af.af",
    "gnomad_genome.af.af_afr",
    "snpeff.ann.gene_id",
    "snpeff.ann.effect",
    "snpeff.ann.putative_impact",
    "snpeff.ann.feature_id",
    "snpeff.ann.hgvs_p",
    "snpeff.ann.protein.length",
    "clinvar.rcv.clinical_significance",
]


def _write_source(path: Path, delimiter: str, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream, delimiter=delimiter)
        writer.writerow(HEADER)
        writer.writerows(rows)


def _write_variant_index(path: Path, payload: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream)


def _load_build_script():
    repo_root = Path(__file__).resolve().parents[1]
    path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "build_association_evidence_v2.py"
    )
    spec = importlib.util.spec_from_file_location("build_association_evidence_v2", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_script_writes_serving_checksum_contract(tmp_path: Path) -> None:
    module = _load_build_script()
    database = tmp_path / "association-evidence-v2.duckdb"
    database.write_bytes(b"normalized-sidecar")
    digest = module.sha256_file(database)

    checksum_path = module.write_sha256_sidecar(database, digest)

    assert checksum_path.name == "association-evidence-v2.duckdb.sha256"
    assert checksum_path.read_text() == f"{digest}  {database.name}\n"


def _builder(tmp_path: Path) -> EvidenceV2Builder:
    cvd_root = tmp_path / "raw" / "cvd"
    trait_root = tmp_path / "raw" / "trait"
    row_base = [
        "rs1",
        "1e-8",
        "hypertension",
        "Study A",
        "PMID: 1",
        "b37",
        "rs1",
        "155",
        "['A', 'G']",
        "1",
        "100",
        "100",
        "snp",
        "0.2",
        "0.3",
        "GENE1",
        "missense_variant",
        "MODERATE",
        "ENST1",
        "p.A1G",
        "100",
        "Pathogenic",
    ]
    synonymous = row_base.copy()
    synonymous[16] = "synonymous_variant"
    synonymous[17] = "LOW"
    synonymous[18] = "ENST2"
    synonymous[19] = "p.A1A"
    synonymous[21] = "Benign"
    alternate_protein_length = row_base.copy()
    alternate_protein_length[20] = "101"
    _write_source(
        cvd_root / "hypertension.txt",
        ",",
        [row_base, synonymous, alternate_protein_length],
    )
    _write_source(trait_root / "rr_interval.txt", "\t", [row_base])
    variant_root = tmp_path / "variant_index"
    _write_variant_index(
        variant_root / "CVD" / "TTN.json.gz",
        [
            {
                "variant_id": "rsMVP",
                "gene_id": "TTN",
                "dataset_type": "CVD",
                "phenotype": "cardiomyopathy",
                "phenotype_path": ["cardiomyopathies", "cardiomyopathy"],
                "source": "million_veteran_program",
                "sources": ["million_veteran_program"],
                "p_value": 1e-9,
                "variation_type": "SNP",
            }
        ],
    )
    tree = tmp_path / "phenotype_tree.json"
    tree.write_text(
        json.dumps(
            {
                "CVD": {"vascular_diseases": {"hypertensive_disorders": ["hypertension"]}},
                "TRAIT": {"ecg_measures": ["hypertension"]},
            }
        )
    )
    return EvidenceV2Builder(
        cvd_root=cvd_root,
        trait_root=trait_root,
        variant_index_root=variant_root,
        output_db=tmp_path / "evidence.duckdb",
        checkpoint_path=tmp_path / "checkpoint.json",
        phenotype_tree=tree,
        release_id="test-v2",
        command="pytest fixture",
        threads=1,
        memory_limit="1GB",
        smoke_files=1,
        progress_interval=1,
    )


def test_allele_derived_variation_type_never_assigns_roles() -> None:
    result = variation_type_from_alleles(["A", "G"])
    assert result["value"] == "SNV"
    assert result["status"] == "derived"
    assert "roles remain unresolved" in result["reason"]
    assert variation_type_from_alleles(["A"])["status"] == "unresolved"


def test_full_separation_and_provenance_contract(tmp_path: Path) -> None:
    builder = _builder(tmp_path)
    result = builder.run()
    assert result["counts"]["provider_records"] == 4
    connection = duckdb.connect(str(tmp_path / "evidence.duckdb"), read_only=True)

    provider_ids = connection.execute(
        "SELECT provider_record_id, source_line, source_row_sha256 FROM provider_records ORDER BY provider_record_id"
    ).fetchall()
    assert len(provider_ids) == len({row[0] for row in provider_ids}) == 4
    assert {row[1] for row in provider_ids} == {2, 3, 4}
    assert all(len(row[2]) == 64 for row in provider_ids)

    consequences = connection.execute(
        "SELECT consequence, severity_selection, provider_record_count FROM consequence_annotations ORDER BY consequence"
    ).fetchall()
    assert {row[0] for row in consequences} == {"missense_variant", "synonymous_variant"}
    assert sum(row[0] == "missense_variant" for row in consequences) == 2
    assert all(row[1] == "not_performed" for row in consequences)

    assertions = connection.execute(
        """SELECT clinical_significance, normalized_term, display_group,
                  assertion_source, condition_status, evidence_granularity,
                  provenance_limitation, raw_source_values_json,
                  provider_record_count
           FROM clinical_assertions
           ORDER BY normalized_term, assertion_source"""
    ).fetchall()
    assert [(row[0], row[3]) for row in assertions] == [
        ("Benign", "legacy_cvd_raw"),
        ("Pathogenic", "legacy_cvd_raw"),
        ("Pathogenic", "legacy_trait_raw"),
    ]
    assert all(row[0] == row[1] for row in assertions)
    assert {row[2] for row in assertions} == {"Benign spectrum", "Pathogenic spectrum"}
    assert all(row[4] == "unavailable" for row in assertions)
    assert all(row[5] == "legacy_variant_level_annotation" for row in assertions)
    assert all("may not refer to the selected HBP phenotype" in row[6] for row in assertions)
    assert all(json.loads(row[7]) for row in assertions)
    assert all(row[8] > 0 for row in assertions)
    assert connection.execute(
        """SELECT count(*)=count(DISTINCT variant_id || chr(31) || normalized_term || chr(31) || assertion_source)
           FROM clinical_assertions"""
    ).fetchone() == (True,)

    mvp = connection.execute(
        """
        SELECT provider_detail_status, provider_detail_reason, missing_fields_json,
               logical_artifact, publication_mode
        FROM source_summary_artifacts WHERE source='million_veteran_program'
        """
    ).fetchone()
    assert mvp[0] == "not_applicable"
    assert mvp[1] is None
    assert "effect_allele" in json.loads(mvp[2])
    assert mvp[3].endswith("TTN.json.gz")
    assert mvp[4] == "gene_scoped_on_demand"
    assert connection.execute(
        "SELECT count(*) FROM association_records WHERE source='million_veteran_program'"
    ).fetchone() == (0,)

    recoverable = connection.execute(
        """
        SELECT effect_allele_status, effect_size_status, standard_error_status,
               sample_size_status, ancestry_status, fine_mapping_status,
               missing_fields_json
        FROM association_records
        WHERE source='legacy_cvd_raw'
        ORDER BY association_record_id
        LIMIT 1
        """
    ).fetchone()
    assert recoverable[:6] == ("unavailable",) * 6
    assert set(json.loads(recoverable[6])) == {
        "effect_allele",
        "effect_size",
        "standard_error",
        "sample_size",
        "ancestry",
        "fine_mapping",
    }

    summary = connection.execute(
        """
        SELECT minimum_reported_p_value, minimum_reported_p_value_association_record_id,
               consequence_conflict, clinical_significance_conflict
        FROM variant_phenotype_summaries
        WHERE gene_id='GENE1' AND dataset_type='CVD' AND variant_id='rs1'
        """
    ).fetchone()
    assert summary[0] == 1e-8
    supplier = connection.execute(
        "SELECT reported_p_value FROM association_records WHERE association_record_id=?",
        [summary[1]],
    ).fetchone()
    assert supplier == (1e-8,)
    assert summary[2] is True
    assert summary[3] is True

    qa = connection.execute("SELECT status FROM qa_results").fetchall()
    assert qa and {row[0] for row in qa} == {"passed"}
    connection.close()


def test_resume_keeps_provider_ids_and_counts_stable(tmp_path: Path) -> None:
    builder = _builder(tmp_path)
    first = builder.run()
    connection = duckdb.connect(str(tmp_path / "evidence.duckdb"), read_only=True)
    first_ids = connection.execute(
        "SELECT provider_record_id FROM provider_records ORDER BY provider_record_id"
    ).fetchall()
    connection.close()

    second = _builder(tmp_path).run()
    connection = duckdb.connect(str(tmp_path / "evidence.duckdb"), read_only=True)
    second_ids = connection.execute(
        "SELECT provider_record_id FROM provider_records ORDER BY provider_record_id"
    ).fetchall()
    connection.close()
    assert first["counts"] == second["counts"]
    assert first_ids == second_ids


def test_clean_rebuild_keeps_provider_ids_and_source_lines_stable(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first = _builder(first_root)
    second = _builder(second_root)

    # The fixture factory writes identical logical source files below each root.
    first.run()
    second.run()
    first_connection = duckdb.connect(str(first_root / "evidence.duckdb"), read_only=True)
    second_connection = duckdb.connect(str(second_root / "evidence.duckdb"), read_only=True)
    query = """
        SELECT provider_record_id, source_file_logical, source_line, source_row_sha256
        FROM provider_records ORDER BY provider_record_id
    """
    assert first_connection.execute(query).fetchall() == second_connection.execute(query).fetchall()
    first_connection.close()
    second_connection.close()


def test_variant_builder_streams_past_insert_batch_boundary(tmp_path: Path) -> None:
    cvd_root = tmp_path / "raw" / "cvd"
    trait_root = tmp_path / "raw" / "trait"
    rows = []
    for index in range(10005):
        rows.append(
            [
                f"rs{index}",
                "1e-8",
                "hypertension",
                "Study A",
                "1",
                "b37",
                f"rs{index}",
                "155",
                "['A', 'G']",
                "1",
                str(index),
                str(index),
                "snp",
                "",
                "",
                "GENE1",
                "missense_variant",
                "MODERATE",
                "ENST1",
                "p.A1G",
                "100",
                "",
            ]
        )
    _write_source(cvd_root / "hypertension.txt", ",", rows)
    _write_source(trait_root / "rr_interval.txt", "\t", [rows[0]])
    variant_root = tmp_path / "variant_index"
    (variant_root / "CVD").mkdir(parents=True)
    (variant_root / "TRAIT").mkdir(parents=True)
    tree = tmp_path / "phenotype_tree.json"
    tree.write_text(
        json.dumps(
            {
                "CVD": {"vascular_diseases": {"hypertensive_disorders": ["hypertension"]}},
                "TRAIT": {"ecg_measures": ["hypertension"]},
            }
        )
    )
    builder = EvidenceV2Builder(
        cvd_root=cvd_root,
        trait_root=trait_root,
        variant_index_root=variant_root,
        output_db=tmp_path / "evidence.duckdb",
        checkpoint_path=tmp_path / "checkpoint.json",
        phenotype_tree=tree,
        release_id="batch-boundary-test",
        command="pytest fixture",
        threads=1,
        memory_limit="1GB",
        progress_interval=1,
    )
    result = builder.run()
    assert result["counts"]["variants"] == 10005
    connection = duckdb.connect(str(tmp_path / "evidence.duckdb"), read_only=True)
    assert connection.execute(
        "SELECT count(*) FROM variants WHERE variation_type='SNV'"
    ).fetchone() == (10005,)
    connection.close()



def test_partitioned_serving_preserves_provider_rows_and_associations(tmp_path: Path) -> None:
    builder = _builder(tmp_path)
    result = builder.run()
    build_script = _load_build_script()
    source_db = tmp_path / "evidence.duckdb"
    source_digest = build_script.sha256_file(source_db)
    build_script.write_sha256_sidecar(source_db, source_digest)
    coarse_root = tmp_path / "serving-coarse"
    coarse_manifest = AssociationEvidenceV2ServingBuilder(
        source_db=tmp_path / "evidence.duckdb",
        intermediate_root=tmp_path / "duckdb_tmp",
        output_root=coarse_root,
        memory_limit="1GB",
        threads=1,
        variant_bucket_characters=2,
        progress_interval=1,
    ).build()
    passthrough_names = (
        "source_summary_associations_by_gene",
        "source_summary_association_phenotype_counts_by_gene",
        "source_summary_association_base_by_gene",
    )
    for logical_name in passthrough_names:
        table_root = coarse_root / "tables" / logical_name / "gene_bucket=00"
        table_root.mkdir(parents=True)
        (table_root / "part-0.parquet").write_bytes(b"fixture")
        coarse_manifest["tables"][logical_name] = {
            "path": f"tables/{logical_name}",
            "partition_key": "gene_id",
            "bucket_function": "sha256(gene_id)[0:2]",
            "bucket_characters": 2,
            "rows": 1,
            "files": 1,
        }
    (coarse_root / "serving-manifest.json").write_text(
        json.dumps(coarse_manifest, indent=2, sort_keys=True) + "\n"
    )
    serving_root = tmp_path / "serving"
    manifest = AssociationEvidenceV2ServingBuilder(
        source_db=tmp_path / "evidence.duckdb",
        intermediate_root=tmp_path / "duckdb_tmp",
        output_root=serving_root,
        memory_limit="1GB",
        threads=1,
        variant_bucket_characters=3,
        progress_interval=1,
        coarse_serving_root=coarse_root,
    ).build()
    finer_root = tmp_path / "serving-finer"
    finer_manifest = AssociationEvidenceV2ServingBuilder(
        source_db=tmp_path / "evidence.duckdb",
        intermediate_root=tmp_path / "duckdb_tmp",
        output_root=finer_root,
        memory_limit="1GB",
        threads=1,
        variant_bucket_characters=4,
        progress_interval=1,
        coarse_serving_root=serving_root,
    ).build()
    for logical_name in passthrough_names:
        assert manifest["tables"][logical_name]["rows"] == 1
        assert finer_manifest["tables"][logical_name]["rows"] == 1
        coarse_file = next((coarse_root / "tables" / logical_name).rglob("*.parquet"))
        serving_file = next((serving_root / "tables" / logical_name).rglob("*.parquet"))
        finer_file = next((finer_root / "tables" / logical_name).rglob("*.parquet"))
        assert coarse_file.stat().st_ino == serving_file.stat().st_ino
        assert serving_file.stat().st_ino == finer_file.stat().st_ino
    assert manifest["contract"] == "association_evidence_v2_partitioned_serving"
    assert manifest["source_db_sha256"] == source_digest
    assert manifest["tables"]["provider_records"]["normalized_source_rows"] == result["counts"]["provider_records"]
    assert manifest["tables"]["provider_records"]["excluded_unkeyed_rows"] == 0
    assert manifest["tables"]["provider_records"]["rows"] == result["counts"]["provider_records"]
    assert manifest["tables"]["association_records"]["rows"] == result["counts"]["association_records"]
    assert (
        manifest["tables"]["variant_phenotype_summaries"]["rows"]
        == result["counts"]["variant_phenotype_summaries"]
    )
    assert manifest["schema_version"] == "2.6.0-rc1"
    assert manifest["variant_bucket_count"] == 4096
    assert finer_manifest["variant_bucket_count"] == 65536
    assert manifest["tables"]["provider_records"]["bucket_characters"] == 3
    assert manifest["tables"]["association_records"]["bucket_characters"] == 3
    assert manifest["tables"]["variant_phenotype_summaries"]["bucket_characters"] == 3
    for logical_name in (
        "consequence_annotations",
        "clinical_assertions",
        "population_observations",
    ):
        assert manifest["tables"][logical_name]["bucket_characters"] == 3
        assert manifest["tables"][logical_name]["directory_layout"] == "nested_coarse_fine"
    assert manifest["tables"]["association_records"]["directory_layout"] == "nested_coarse_fine"
    assert any(
        path.name.startswith("coarse_bucket=")
        for path in (serving_root / "tables" / "association_records").iterdir()
    )
    for logical_name in (
        "provider_records",
        "association_records",
        "variant_phenotype_summaries",
        "consequence_annotations",
        "clinical_assertions",
        "population_observations",
    ):
        assert finer_manifest["tables"][logical_name]["bucket_characters"] == 4
        assert finer_manifest["tables"][logical_name]["directory_layout"] == "nested_coarse_fine"
    assert (
        manifest["tables"]["consequence_annotations_by_gene"]["rows"]
        == result["counts"]["consequence_annotations"]
    )
    connection = duckdb.connect()
    provider_rows = connection.execute(
        f"SELECT provider_record_id, source_line, source_row_sha256 "
        f"FROM read_parquet('{serving_root / 'tables/provider_records/**/*.parquet'}') "
        "ORDER BY provider_record_id"
    ).fetchall()
    association_rows = connection.execute(
        f"SELECT association_record_id, variant_id, reported_p_value "
        f"FROM read_parquet('{serving_root / 'tables/association_records/**/*.parquet'}') "
        "ORDER BY association_record_id"
    ).fetchall()
    finer_association_rows = connection.execute(
        f"SELECT association_record_id, variant_id, reported_p_value "
        f"FROM read_parquet('{finer_root / 'tables/association_records/**/*.parquet'}') "
        "ORDER BY association_record_id"
    ).fetchall()
    gene_consequences = connection.execute(
        f"SELECT gene_id, variant_id, consequence "
        f"FROM read_parquet('{serving_root / 'tables/consequence_annotations_by_gene/**/*.parquet'}') "
        "ORDER BY gene_id, variant_id, consequence"
    ).fetchall()
    assert len(provider_rows) == result["counts"]["provider_records"]
    assert all(row[1] >= 2 and len(row[2]) == 64 for row in provider_rows)
    assert len(association_rows) == result["counts"]["association_records"]
    assert finer_association_rows == association_rows
    assert len(gene_consequences) == result["counts"]["consequence_annotations"]
    assert {row[0] for row in gene_consequences} == {"GENE1"}
    connection.close()
