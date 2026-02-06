from src.datachain.query.validator import QueryValidator
from src.datachain.query.types import QueryContext
from .fixtures.semantic_fixture import semantic_model
from.fixtures.validation_queries import (
    no_selects_has_filters,
    no_selects_has_orderby,
    has_selects
)

validator = QueryValidator()
ctx = QueryContext()

# Structure Validations
def test_query_has_no_select_invalid():
    errors = validator.validate_structure(no_selects_has_filters, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "structure_validation"

    errors = validator.validate_structure(no_selects_has_orderby, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "structure_validation"

def test_query_has_select_items_valid():
    errors = validator.validate_structure(has_selects, ctx)
    assert len(errors) == 0


# Reference Validation
def test_query_misreferences_kpi_invalid():
    ...

def test_query_misreferences_filter_invalid():
    ...

def test_query_misreferences_dimension_invalid():
    ...

def test_query_applies_timegrain_to_none_date_dimension_invalid():
    ...

def test_query_misreferences_measure_invalid():
    ...

def test_query_misreferences_dimension_filter_invalid():
    ...

def test_query_has_all_references_correct_valid():
    ...