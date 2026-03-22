import gzip
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.annotations import GtfGeneAnnotationIndex


def _write_gtf(path: Path) -> None:
    payload = "\n".join(
        [
            "##description: test",
            '\t'.join(
                [
                    "1",
                    "HAVANA",
                    "gene",
                    "100",
                    "400",
                    ".",
                    "+",
                    ".",
                    'gene_id "ENSG000001.5"; gene_name "GENE1"; gene_type "protein_coding";',
                ]
            ),
            '\t'.join(
                [
                    "1",
                    "HAVANA",
                    "transcript",
                    "100",
                    "250",
                    ".",
                    "+",
                    ".",
                    'gene_id "ENSG000001.5"; gene_name "GENE1"; transcript_id "ENST000001.2"; transcript_name "GENE1-201"; transcript_type "protein_coding"; tag "basic"; tag "Ensembl_canonical";',
                ]
            ),
            '\t'.join(
                [
                    "1",
                    "HAVANA",
                    "transcript",
                    "120",
                    "400",
                    ".",
                    "+",
                    ".",
                    'gene_id "ENSG000001.5"; gene_name "GENE1"; transcript_id "ENST000002.1"; transcript_name "GENE1-202"; transcript_type "protein_coding"; tag "basic";',
                ]
            ),
        ]
    )
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(payload)


def test_gtf_gene_annotation_index_parses_overlap_and_lookup(tmp_path: Path) -> None:
    gtf_path = tmp_path / "gencode.test.annotation.gtf.gz"
    _write_gtf(gtf_path)

    index = GtfGeneAnnotationIndex(path=gtf_path, bin_size_bp=50, progress_every_lines=1)

    overlaps = index.overlap_region_genes(chromosome="1", start=180, end=220)
    assert overlaps == [
        {
            "id": "ENSG000001",
            "external_name": "GENE1",
            "start": 100,
            "end": 400,
            "strand": 1,
            "biotype": "protein_coding",
        }
    ]

    lookup = index.gene_lookup("ENSG000001")
    assert lookup["id"] == "ENSG000001"
    assert lookup["display_name"] == "GENE1"
    assert lookup["Transcript"][0]["id"] == "ENST000001"
    assert lookup["Transcript"][0]["is_canonical"] == 1
