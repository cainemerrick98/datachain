from src.datachain.query.validator import QueryValidator
from src.datachain.query.types import QueryContext
from .fixtures.semantic_fixture import semantic_model
from.fixtures import validation_queries as vq

validator = QueryValidator()
ctx = QueryContext()

# Structure Validations
def test_query_has_no_select_invalid():
    errors = validator.validate_structure(vq.no_selects_has_filters, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "structure_validation"

    errors = validator.validate_structure(vq.no_selects_has_orderby, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "structure_validation"

def test_query_has_select_items_valid():
    errors = validator.validate_structure(vq.has_selects, ctx)
    assert len(errors) == 0


# Reference Validation
def test_query_misreferences_kpi_invalid():
    errors = validator.validate_references(vq.invalid_kpi_reference, semantic_model, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "reference_validation"

def test_query_misreferences_filter_invalid():
    errors = validator.validate_references(vq.invalid_kpi_reference, semantic_model, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "reference_validation"

def test_query_misreferences_dimension_invalid():
    errors = validator.validate_references(vq.invalid_dimension_table, semantic_model, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "reference_validation"
    errors = validator.validate_references(vq.invalid_dimension_column, semantic_model, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "reference_validation"

def test_query_applies_timegrain_to_none_date_dimension_invalid():
    errors = validator.validate_references(vq.invalid_timegrain, semantic_model, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "reference_validation"

def test_query_misreferences_measure_invalid():
    errors = validator.validate_references(vq.invalid_measure, semantic_model, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "reference_validation"

def test_query_misreferences_dimension_filter_invalid():
    errors = validator.validate_references(vq.invalid_dimension_filter, semantic_model, ctx)
    assert len(errors) == 1
    assert errors[0].stage == "reference_validation"

def test_query_has_all_references_correct_valid():
    errors = validator.validate_references(vq.valid_query, semantic_model, ctx)
    assert len(errors) == 0

# Join Path Validation
def test_valid_join_path_product_customer():
    ctx, common = vq.valid_ctx_sales_common
    errors = validator.validate_join_path(semantic_model, ctx)

    assert len(errors) == 0
    assert ctx.common_table == common

def test_valid_join_path_product_component():
    ctx, common = vq.valid_ctx_component_common
    errors = validator.validate_join_path(semantic_model, ctx)

    assert len(errors) == 0
    assert ctx.common_table == common

def test_invalid_join_path_no_common():
    ctx, common = vq.invalid_ctx_no_common
    errors = validator.validate_join_path(semantic_model, ctx)

    print(ctx.common_table)

    assert len(errors) == 1
