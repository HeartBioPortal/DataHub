import json
from pathlib import Path


ALLOWED_CATEGORIES = {
    "cvd_focused_portals",
    "gwas_statistical_genetics",
    "population_reference_variation",
    "clinical_variant_interpretation",
    "bulk_transcriptomics_qtl",
    "single_cell_spatial",
    "epigenomics_regulatory",
    "proteomics",
    "metabolomics_lipidomics",
    "pathways_interactions_networks",
    "drug_target_translational",
    "ontologies_standards",
}


def test_source_catalog_manifests_have_required_metadata() -> None:
    source_dir = Path(__file__).resolve().parents[1] / "config" / "sources"
    manifests = sorted(source_dir.glob("*.json"))

    # Ensure we maintain a broad multi-category source catalog beyond the
    # currently integrated adapters.
    assert len(manifests) >= 30

    categories_seen: set[str] = set()
    for manifest_path in manifests:
        payload = json.loads(manifest_path.read_text())
        assert payload["source_id"]
        assert payload["display_name"]
        assert payload["adapter_name"]
        assert payload["dataset_types"]
        assert payload["modalities"]
        assert payload["access_mode"] in {"api", "download", "scrape", "hybrid"}
        assert payload["integration_status"] in {"integrated", "catalog_only"}

        category = payload["data_category"]
        assert category in ALLOWED_CATEGORIES
        categories_seen.add(category)

    assert categories_seen == ALLOWED_CATEGORIES
