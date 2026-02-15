import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub import DatasetProfileLoader, MissingFieldStrategy  # noqa: E402


def test_profile_loader_lists_default_profiles() -> None:
    loader = DatasetProfileLoader()
    names = loader.list_profiles()

    assert "association" in names
    assert "expression" in names
    assert "single_cell" in names


def test_association_profile_loads_with_axis_policy() -> None:
    loader = DatasetProfileLoader()
    profile = loader.load("association")

    assert profile.dataset_type == "ASSOCIATION"
    assert "gene_id" in profile.required_fields

    contract = profile.to_contract()
    assert contract.policy_for("gene_id").required is True
    assert (
        contract.policy_for("variation_type").missing_strategy
        == MissingFieldStrategy.EXCLUDE
    )


def test_single_cell_profile_contains_cell_type_requirement() -> None:
    loader = DatasetProfileLoader()
    profile = loader.load("single_cell")

    contract = profile.to_contract()
    assert contract.policy_for("metadata.cell_type").required is True
