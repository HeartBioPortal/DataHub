"""Input adapters for DataHub."""

from .base import DataAdapter
from .clinvar import ClinVarAssociationAdapter
from .ensembl import EnsemblAssociationAdapter
from .gwas import GWASAssociationAdapter
from .legacy_csv import LegacyAssociationCsvAdapter
from .phenotypes import PhenotypeMapper

__all__ = [
    "DataAdapter",
    "LegacyAssociationCsvAdapter",
    "GWASAssociationAdapter",
    "EnsemblAssociationAdapter",
    "ClinVarAssociationAdapter",
    "PhenotypeMapper",
]
