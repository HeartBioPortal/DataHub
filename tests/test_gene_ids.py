from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.gene_ids import is_valid_gene_id, sql_has_gene_like_identifier


def test_is_valid_gene_id_accepts_gene_like_tokens() -> None:
    assert is_valid_gene_id("TTN")
    assert is_valid_gene_id("RPS20P20")
    assert is_valid_gene_id("C1orf220")
    assert is_valid_gene_id("ENSG00000155657")


def test_is_valid_gene_id_rejects_numeric_leaks() -> None:
    assert not is_valid_gene_id("")
    assert not is_valid_gene_id(None)
    assert not is_valid_gene_id("0.799091")
    assert not is_valid_gene_id("12345")


def test_sql_gene_like_predicate_mentions_regex() -> None:
    predicate = sql_has_gene_like_identifier("p.gene_id")
    assert "regexp_matches" in predicate
    assert "p.gene_id" in predicate
