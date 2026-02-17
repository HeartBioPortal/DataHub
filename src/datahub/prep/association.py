"""Raw association data preparation and normalization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from datahub.prep.profiles import RawAssociationPreparationProfile


PREPARED_ASSOCIATION_COLUMNS: tuple[str, ...] = (
    "dataset_type",
    "source_name",
    "marker_id",
    "study_id",
    "study",
    "pmid",
    "study_genome_build",
    "rsid",
    "pval",
    "gene",
    "phenotype",
    "functional_class",
    "var_class",
    "clinical_significance",
    "most_severe_consequence",
    "allele_string",
    "protein_start",
    "protein_end",
    "ancestry_data",
)


@dataclass
class PreparationReport:
    """Summary of one preparation run."""

    input_rows: int
    output_rows: int
    dropped_rows: int
    output_path: Path


class AssociationRawPreparer:
    """Convert non-standard raw association CSVs into a stable prepared schema."""

    def __init__(
        self,
        *,
        profile: RawAssociationPreparationProfile,
        chunksize: int = 100_000,
        ancestry_precision: int | None = 6,
    ) -> None:
        self.profile = profile
        self.chunksize = chunksize
        self.ancestry_precision = ancestry_precision

    def prepare_csv(
        self,
        *,
        input_csv: str | Path,
        output_csv: str | Path,
    ) -> PreparationReport:
        """Prepare one raw CSV into a normalized CSV schema."""

        input_path = Path(input_csv)
        output_path = Path(output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        input_rows = 0
        output_rows = 0
        dropped_rows = 0
        wrote_header = False

        for frame in pd.read_csv(input_path, chunksize=self.chunksize):
            input_rows += len(frame)
            prepared_rows: list[dict[str, Any]] = []

            for row in frame.to_dict(orient="records"):
                prepared = self._prepare_row(row)
                if prepared is None:
                    dropped_rows += 1
                    continue
                prepared_rows.append(prepared)

            if not prepared_rows:
                continue

            prepared_frame = pd.DataFrame(prepared_rows, columns=list(PREPARED_ASSOCIATION_COLUMNS))
            prepared_frame.to_csv(
                output_path,
                mode="a" if wrote_header else "w",
                header=not wrote_header,
                index=False,
            )
            wrote_header = True
            output_rows += len(prepared_rows)

        if not wrote_header:
            pd.DataFrame(columns=list(PREPARED_ASSOCIATION_COLUMNS)).to_csv(
                output_path,
                index=False,
            )

        return PreparationReport(
            input_rows=input_rows,
            output_rows=output_rows,
            dropped_rows=dropped_rows,
            output_path=output_path,
        )

    def _prepare_row(self, row: dict[str, Any]) -> dict[str, Any] | None:
        marker_id = self._pick(row, "marker_id")
        rsid = self._derive_rsid(row, marker_id)
        phenotype = self._clean_text(self._pick(row, "phenotype"))
        gene = self._clean_text(self._pick(row, "gene"))

        if not rsid or not phenotype or not gene:
            return None

        allele_string = self._clean_text(self._pick(row, "allele_string"))
        ancestry_data = self._build_ancestry_data(row, allele_string)

        var_class = self._clean_text(self._pick(row, "var_class"))
        if var_class and var_class.lower() == "snp":
            var_class = "SNP"

        pval = self._to_float(self._pick(row, "pval"))

        prepared = {
            "dataset_type": self.profile.dataset_type,
            "source_name": self.profile.source_name,
            "marker_id": marker_id or "",
            "study_id": self._clean_text(self._pick(row, "study_id")) or "",
            "study": self._clean_text(self._pick(row, "study")) or "",
            "pmid": self._clean_text(self._pick(row, "pmid")) or "",
            "study_genome_build": self._clean_text(self._pick(row, "study_genome_build")) or "",
            "rsid": rsid,
            "pval": pval if pval is not None else "",
            "gene": gene,
            "phenotype": phenotype,
            "functional_class": self._clean_text(self._pick(row, "functional_class")) or "",
            "var_class": var_class or "",
            "clinical_significance": self._clean_text(self._pick(row, "clinical_significance")) or "",
            "most_severe_consequence": self._clean_text(self._pick(row, "most_severe_consequence")) or "",
            "allele_string": allele_string or "",
            "protein_start": self._clean_text(self._pick(row, "protein_start")) or "",
            "protein_end": self._clean_text(self._pick(row, "protein_end")) or "",
            "ancestry_data": ancestry_data,
        }
        return prepared

    def _pick(self, row: dict[str, Any], field_name: str) -> Any:
        for candidate in self.profile.candidates_for(field_name):
            if candidate not in row:
                continue
            value = row[candidate]
            if self._clean_text(value) is not None:
                return value

        if field_name in self.profile.defaults:
            return self.profile.defaults[field_name]

        return None

    def _derive_rsid(self, row: dict[str, Any], marker_id: str | None) -> str | None:
        explicit = self._clean_text(self._pick(row, "rsid"))
        if explicit:
            return explicit

        marker = self._clean_text(marker_id)
        if not marker:
            return None

        if marker.startswith("rs"):
            return marker

        return marker.split("-")[0]

    def _build_ancestry_data(self, row: dict[str, Any], allele_string: str | None) -> str:
        ancestry: dict[str, dict[str, Any]] = {}
        for source_col, label in self.profile.ancestry_fields.items():
            if source_col not in row:
                continue
            value = self._to_float(row[source_col])
            if value is None:
                continue
            if self.ancestry_precision is not None:
                value = round(value, self.ancestry_precision)
            ancestry[label] = {
                "change": allele_string or "",
                "MAF": value,
            }

        return json.dumps(ancestry, sort_keys=True)

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        cleaned = str(value).strip()
        if not cleaned or cleaned.lower() in {"nan", "none", "null", "na"}:
            return None
        return cleaned

    @staticmethod
    def _to_float(value: Any) -> float | None:
        text = AssociationRawPreparer._clean_text(value)
        if text is None:
            return None
        try:
            return float(text)
        except (TypeError, ValueError):
            return None
