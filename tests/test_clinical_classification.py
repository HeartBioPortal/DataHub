from __future__ import annotations

import pytest

from datahub.association_evidence_v2.clinical_classification import (
    APPROVED_TERMS,
    display_group_for_term,
    normalized_clinical_terms,
    parse_clinical_classification,
)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("Pathogenic", ["Pathogenic"]),
        ("['Benign', 'Likely benign']", ["Benign", "Likely benign"]),
        ('["Benign", "association"]', ["Benign", "association"]),
        ("[Benign, Likely benign, Benign]", ["Benign", "Likely benign"]),
        ("Pathogenic, other", ["Pathogenic", "other"]),
        ("['Pathogenic, other', 'Benign']", ["Pathogenic", "other", "Benign"]),
        (
            "['Conflicting interpretations of pathogenicity, Affects, other']",
            ["Conflicting interpretations of pathogenicity", "Affects", "other"],
        ),
        ("association; protective", ["association", "protective"]),
        ("Benign / Likely benign", ["Benign/Likely benign"]),
        ("  affects. ", ["Affects"]),
        ("likely_benign", ["Likely benign"]),
    ],
)
def test_audited_clinical_classification_parser(raw_value, expected):
    assert normalized_clinical_terms(raw_value) == expected


def test_repeated_terms_are_removed_only_from_display_projection():
    parsed = parse_clinical_classification("[Benign, Benign, Likely benign]")
    assert parsed.terms == ("Benign", "Benign", "Likely benign")
    assert parsed.display_terms == ("Benign", "Likely benign")
    assert parsed.duplicate_terms == {"Benign": 2}
    assert parsed.raw_value == "[Benign, Benign, Likely benign]"


def test_unknown_meaningful_term_fails_closed():
    with pytest.raises(ValueError, match="Unparsed clinical-classification"):
        normalized_clinical_terms("new unsupported term")


def test_exact_audited_vocabulary_and_groups():
    assert len(APPROVED_TERMS) == 15
    assert set(APPROVED_TERMS) == {
        "Benign", "Likely benign", "Benign/Likely benign", "Pathogenic",
        "Likely pathogenic", "Pathogenic/Likely pathogenic",
        "Uncertain significance", "Conflicting interpretations of pathogenicity",
        "association", "risk factor", "protective", "drug response", "Affects",
        "other", "not provided",
    }
    assert display_group_for_term("association") == "Association and risk context"
    assert display_group_for_term("not provided") == "Details only"
