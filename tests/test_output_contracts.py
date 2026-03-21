import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub import OutputContractLoader


def test_output_contract_loader_reads_structural_variant_legacy_contract() -> None:
    loader = OutputContractLoader()
    contract = loader.load("structural_variant_legacy")

    assert contract.name == "structural_variant_legacy"
    assert "gene_defaults" in contract.payload
    assert "variant_defaults" in contract.payload
    assert contract.payload["gene_defaults"]["canonical_transcript"] == []
    assert contract.payload["variant_defaults"]["phenotype"] == []
