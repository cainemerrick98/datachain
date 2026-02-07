from src.datachain.query.resolver import QueryResolver
from src.datachain.query.types import QueryContext
from .fixtures.semantic_fixture import semantic_model
from .fixtures import resolver_queries_expected

resolver = QueryResolver()
ctx = QueryContext()

def test_resolve_kpi_returns_correct_bi_measure():
    query, expected = resolver_queries_expected.kpi_and_resolved_measure
    result = resolver.resolve(query, semantic_model, ctx)

    assert result == expected


def test_resolve_semantic_filter_dimension_filter():
    """Returns dimension filter when filter is semantic comparison"""
    query, expected = resolver_queries_expected.semantic_filter_and_dimension_filter
    result = resolver.resolve(query, semantic_model, ctx)

    assert result == expected

def test_resolve_semantic_filter_measure_filter():
    """Returns measure filter when filter is kpi comparison"""
    query, expected = resolver_queries_expected.semantic_filter_and_measure_filter
    result = resolver.resolve(query, semantic_model, ctx)

    assert result == expected

def test_resolve_measure_filter():
    query, expected = resolver_queries_expected.measure_filter_and_resolved_measure_filter
    result = resolver.resolve(query, semantic_model, ctx)

    assert result == expected

def test_resolve_dimension_filter():
    query, expected = resolver_queries_expected.dimension_and_resolved_dimension_filter
    result = resolver.resolve(query, semantic_model, ctx)

    assert result == expected
    

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