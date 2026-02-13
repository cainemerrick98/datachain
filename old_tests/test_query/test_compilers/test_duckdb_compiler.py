from src.datachain.query.compilers.duckdb import DuckDbCompiler
from .fixtures import queries

def test_compiler_simplest_sql():
    result = DuckDbCompiler.compile(queries.simplest_sql_query)
    print(result)


def test_compiler_agg_group_by_sql():
    result = DuckDbCompiler.compile(queries.simple_agg_and_groupby_sql_query)
    print(result)