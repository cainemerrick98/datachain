from src.datachain.query.validator import QueryValidator
from .fixtures.semantic_fixture import semantic_model

# Structure Validations
def query_has_no_select_invalid():
    ...


def query_has_select_items_valid():
    ...

# Reference Validation
def query_misreferences_kpi_invalid():
    ...

def query_misreferences_filter_invalid():
    ...

def query_misreferences_dimension_invalid():
    ...

def query_applies_timegrain_to_none_date_dimension_invalid():
    ...

def query_misreferences_measure_invalid():
    ...

def query_misreferences_dimension_filter_invalid():
    ...

def query_has_all_references_correct_valid():
    ...