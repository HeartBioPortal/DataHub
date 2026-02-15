"""Contract validation for canonical DataHub records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from datahub.config import DatasetContract, MissingFieldStrategy
from datahub.models import CanonicalRecord


@dataclass(frozen=True)
class ValidationIssue:
    """Describes why a record was modified or dropped during validation."""

    record_key: tuple[str, str, str, str]
    field_name: str
    message: str


@dataclass
class ValidationResult:
    """Validation output containing cleaned records and validation diagnostics."""

    records: list[CanonicalRecord] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)
    dropped: int = 0


class ContractValidator:
    """Apply a dataset contract to canonical records.

    Required fields can either drop records, be replaced with an explicit unknown
    marker, or be accepted as-is, depending on the field policy.
    """

    def validate(
        self,
        records: list[CanonicalRecord],
        contract: DatasetContract,
    ) -> ValidationResult:
        result = ValidationResult()

        for record in records:
            keep_record = True
            for field_name, policy in contract.field_policies.items():
                value = self._get_value(record, field_name)
                has_value = value is not None and str(value).strip() != ""

                if has_value:
                    continue

                if not policy.required:
                    if policy.missing_strategy is MissingFieldStrategy.UNKNOWN:
                        self._set_value(record, field_name, policy.unknown_value)
                    continue

                if policy.missing_strategy is MissingFieldStrategy.UNKNOWN:
                    self._set_value(record, field_name, policy.unknown_value)
                    result.issues.append(
                        ValidationIssue(
                            record_key=record.key(),
                            field_name=field_name,
                            message="Required field missing; set to unknown marker.",
                        )
                    )
                    continue

                if policy.missing_strategy is MissingFieldStrategy.ALLOW:
                    result.issues.append(
                        ValidationIssue(
                            record_key=record.key(),
                            field_name=field_name,
                            message="Required field missing; kept by policy.",
                        )
                    )
                    continue

                keep_record = False
                result.issues.append(
                    ValidationIssue(
                        record_key=record.key(),
                        field_name=field_name,
                        message="Required field missing; record excluded.",
                    )
                )
                break

            if keep_record:
                result.records.append(record)
            else:
                result.dropped += 1

        return result

    @staticmethod
    def _get_value(record: CanonicalRecord, field_name: str) -> Any:
        return getattr(record, field_name, None)

    @staticmethod
    def _set_value(record: CanonicalRecord, field_name: str, value: Any) -> None:
        setattr(record, field_name, value)
