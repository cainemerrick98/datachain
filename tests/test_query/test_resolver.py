from src.datachain.query.resolver import QueryResolver
from .fixtures.semantic_fixture import semantic_model

def test_resolve_kpi_returns_correct_bi_measure():
    ...

def test_resolve_semantic_filter_dimension_filter():
    """Returns dimension filter when filter is semantic comparison"""
    ...

def test_resolve_semantic_filter_measure_filter():
    """Returns measure filter when filter is kpi comparison"""
    ...

def test_resolve_measure_filter():
    ...

def test_resolve_measure_filter_measure_doesnt_exist():
    ...

def test_resolve_dimension_filter():
    ...

def test_resolve_orderby_dimension():
    ...

def test_resolve_orderby_kpi():
    ...

def test_resolve_orderby_measure():
    ...

def test_resolve_dimensions():
    """Tests the splitting of dimensions into time and none time grain"""

def test_resolve():
    """Full resolve method tested against a query, sm and ctx"""