from biquery import BIQuery
from ..errors import DataChainError

def validate_biquery(biquery: BIQuery) -> list[DataChainError]:
    """Validates the structure of the biquery"""
    errors = []
    if not any([biquery.dimensions, biquery.metrics]):
        errors.append(
            DataChainError(
                stage="validate_structure",
                code="no_dimensions_or_metrics",
                message="A BIQuery must have at least one dimension or metric.",
                hint="Add at least one dimension or metric to the BIQuery.",
            )
        )
    
    return errors
