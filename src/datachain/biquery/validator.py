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

    biquery.orderby = [(col, direction.lower()) for col, direction in biquery.orderby]
    for col, direction in biquery.orderby:
        if direction not in ("asc", "desc"):
            errors.append(
                DataChainError(
                    stage="validate_structure",
                    code="invalid_orderby_direction",
                    message=f"Invalid sorting direction '{direction}' for column '{col}'. Must be 'asc' or 'desc'.",
                    hint="Ensure that all sorting directions in the orderby clause are either 'asc' or 'desc'.",
                )
            )
        
    
    return errors
