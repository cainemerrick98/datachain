from src.datachain.query.planner import QueryPlanner
from .fixtures.semantic_fixture import semantic_model
from .fixtures import planner_queries
from src.datachain.query.types import QueryContext
from src.datachain.query.models import SQLQuery

planner = QueryPlanner()
graph = semantic_model.get_relationship_graph()

def test_find_join_path_to_common_table_product_customer():
    joins = planner._find_join_path_to_common_table("Product", "Sales", graph)
    expected = [
        ("Product", "Sales")
    ]
    assert joins == expected

    joins = planner._find_join_path_to_common_table("Customer", "Sales", graph)
    expected = [
        ("Customer", "Sales")
    ]
    assert joins == expected

    
def test_analyse_context_requires_cte():
    """Check the flag and the measure map"""
    ctx = QueryContext()
    ctx.tables = {"Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(planner_queries.requires_cte, ctx, semantic_model)

    assert ctx.requires_cte is True
    assert len(ctx.window_measures) == 1
    assert "change_in_total_revenue" in ctx.window_measure_map


def test_analyse_context_doesnt_require_cte():
    ctx = QueryContext()
    ctx.tables = {"Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(planner_queries.simple_no_window, ctx, semantic_model)

    assert ctx.requires_cte is False
    assert len(ctx.window_measures) == 0

def test_analyse_context_no_measures():
    ctx = QueryContext()
    ctx.tables = {"Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(planner_queries.no_measures, ctx, semantic_model)

    assert ctx.requires_cte is False
    assert len(ctx.unique_measures) == 0

def test_analyse_context_join_path():
    ctx = QueryContext()
    ctx.tables = {"Product", "Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(planner_queries.multi_table, ctx, semantic_model)

    assert ("Product", "Sales") in ctx.joins

def test_plan_simple_query_no_cte():
    ctx = QueryContext()
    ctx.tables = {"Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(planner_queries.simple_no_window, ctx, semantic_model)
    sql = planner.plan(planner_queries.simple_no_window, ctx, semantic_model)

    assert isinstance(sql, SQLQuery)
    assert sql.from_ == "Sales"
    # measure should be present as a select alias
    aliases = [c.alias for c in sql.columns]
    assert "total_revenue" in aliases

def test_plan_big_query_no_cte():
    # reuse multi_table but treat as single-table common_table to avoid missing joins
    ctx = QueryContext()
    ctx.tables = {"Product", "Customer", "Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(planner_queries.multi_table, ctx, semantic_model)
    sql = planner.plan(planner_queries.multi_table, ctx, semantic_model)

    assert isinstance(sql, SQLQuery)
    assert sql.from_ == "Sales"
    assert sql.joins is not None

def test_plan_simple_query_cte():
    ctx = QueryContext()
    ctx.tables = {"Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(planner_queries.requires_cte, ctx, semantic_model)
    sql = planner.plan(planner_queries.requires_cte, ctx, semantic_model)

    # outer query should select from a CTE (SQLQuery) when requires_cte
    assert isinstance(sql.from_, SQLQuery)
    # windowed measure should be exposed in outer columns
    assert any((c.alias and c.alias.startswith("window ")) for c in sql.columns)

def test_plan_big_query_cte():
    # use the multi_table with a windowed measure to force CTE + joins
    # create a copy of requires_cte but include multiple dimensions
    rq = planner_queries.requires_cte.copy()
    rq.dimensions = planner_queries.multi_table.dimensions

    ctx = QueryContext()
    ctx.tables = {"Product", "Customer", "Sales"}
    ctx.common_table = "Sales"

    planner.analyse_context(rq, ctx, semantic_model)
    sql = planner.plan(rq, ctx, semantic_model)

    print(sql)

    assert isinstance(sql.from_, SQLQuery)
    assert sql.joins is None