import importlib.util
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.adapters import PhenotypeMapper


def _load_legacy_ingest_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "ingest_legacy_raw_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("legacy_ingest_script", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_resolve_csv_delimiter_auto_detects_tab(tmp_path: Path) -> None:
    module = _load_legacy_ingest_module()
    sample = tmp_path / "trait.txt"
    sample.write_text("col1\tcol2\tcol3\n1\t2\t3\n")
    assert module._resolve_csv_delimiter("auto", sample) == "tab"


def test_resolve_csv_delimiter_auto_detects_comma(tmp_path: Path) -> None:
    module = _load_legacy_ingest_module()
    sample = tmp_path / "cvd.txt"
    sample.write_text("col1,col2,col3\n1,2,3\n")
    assert module._resolve_csv_delimiter("auto", sample) == "comma"


def test_stage_sql_preserves_gene_column_with_comma_containing_alleles(tmp_path: Path) -> None:
    module = _load_legacy_ingest_module()
    sample = tmp_path / "cvd.txt"
    sample.write_text(
        "\n".join(
            [
                "MarkerID,pval,Phenotype,Study,PMID,StudyGenomeBuild,dbsnp.rsid,dbsnp.dbsnp_build,dbsnp.alleles.allele,dbsnp.chrom,dbsnp.hg19.start,dbsnp.hg19.end,dbsnp.vartype,gnomad_genome.af.af,gnomad_genome.af.af_afr,gnomad_genome.af.af_amr,gnomad_genome.af.af_asj,gnomad_genome.af.af_eas,gnomad_genome.af.af_fin,gnomad_genome.af.af_nfe,gnomad_genome.af.af_oth,snpeff.ann.gene_id,snpeff.ann.effect,snpeff.ann.putative_impact,snpeff.ann.feature_id,snpeff.ann.hgvs_p,snpeff.ann.protein.length,dbnsfp.chrom,dbnsfp.hg18.start,dbnsfp.hg18.end,dbnsfp.hg19.start,dbnsfp.hg19.end,dbnsfp.hg38.start,dbnsfp.hg38.end,dbnsfp.ensembl.proteinid,dbnsfp.ensembl.transcriptid,clinvar.rcv.clinical_significance",
                "rs11757904,0.01204,Mitral_annular_calcification,2013-CHARGE-Mitral_annular_calcification,PMID: 23388002,,rs11757904,120,\"['A', 'C']\",6,11330169,11330169,snp,0.0780078,0.0239954,0.0584726,0.112583,0.00123457,0.121203,0.106275,0.105102,NEDD9,intron_variant,MODIFIER,NM_001142393.1,,,,,,,,,,,,",
            ]
        )
        + "\n"
    )

    con = duckdb.connect()
    module._ensure_slug_macro(con)
    module._create_mapping_table(
        con,
        PhenotypeMapper(mapping={"mitral_annular_calcification": ("CVD", "valvular_heart_disease")}),
    )
    sql, params = module._build_stage_sql(
        file_path=sample,
        dataset_id="legacy_cvd",
        source="legacy_cvd_raw",
        dataset_type="CVD",
        csv_delimiter="comma",
        csv_strict_mode=False,
        csv_ignore_errors=True,
        csv_null_padding=True,
    )
    con.execute(sql, params)
    row = con.execute(
        """
        SELECT gene_id, variation_type, ancestry, ancestry_af
        FROM __legacy_file_rows
        ORDER BY ancestry NULLS LAST
        LIMIT 1
        """
    ).fetchone()
    con.close()

    assert row[0] == "NEDD9"
    assert row[1] == "SNP"
