"""Composable DataHub pipeline orchestrator."""

from __future__ import annotations

from dataclasses import dataclass, field

from datahub.adapters.base import DataAdapter
from datahub.config import DatasetContract
from datahub.enrichment import EnrichmentCandidate, SourcePriorityEnricher
from datahub.models import CanonicalRecord
from datahub.publishers.base import Publisher
from datahub.quality import ContractValidator, ValidationIssue
from datahub.storage.base import CanonicalStorage


@dataclass
class DataHubRunReport:
    """Execution summary for a pipeline run."""

    adapter_count: int
    ingested_records: int
    validated_records: int
    dropped_records: int
    issues: list[ValidationIssue] = field(default_factory=list)


class DataHubPipeline:
    """Run adapters, enrichment, validation, storage and publication in order."""

    def __init__(
        self,
        *,
        contract: DatasetContract,
        adapters: list[DataAdapter],
        validator: ContractValidator | None = None,
        enricher: SourcePriorityEnricher | None = None,
        enrichment_candidates: list[EnrichmentCandidate] | None = None,
        storage: CanonicalStorage | None = None,
        publishers: list[Publisher] | None = None,
    ) -> None:
        self.contract = contract
        self.adapters = adapters
        self.validator = validator or ContractValidator()
        self.enricher = enricher
        self.enrichment_candidates = enrichment_candidates or []
        self.storage = storage
        self.publishers = publishers or []

    def run(self) -> DataHubRunReport:
        records: list[CanonicalRecord] = []

        for adapter in self.adapters:
            records.extend(list(adapter.read()))

        if self.enricher is not None:
            records = self.enricher.enrich(records, self.enrichment_candidates)

        validation = self.validator.validate(records, self.contract)
        valid_records = validation.records

        if self.storage is not None:
            self.storage.persist(valid_records)

        for publisher in self.publishers:
            publisher.publish(valid_records)

        return DataHubRunReport(
            adapter_count=len(self.adapters),
            ingested_records=len(records),
            validated_records=len(valid_records),
            dropped_records=validation.dropped,
            issues=validation.issues,
        )
