from .models import (
    SQLQuery,
    Join,
    GroupBy,
    OrderBy,

    SelectItem,
    QueryColumn,
    SQLMeasure,
    BinaryMetric,

    And,
    Or,
    Not,
    Comparison,
    ColumnComparison,

    BIQuery,


    SemanticModel,

)


def transform_bi_query_to_sql_query(bi: BIQuery, semantic_model: SemanticModel) -> SQLQuery:
    """
    Docstring for transform_bi_query_to_sql_query
    
    :param bi: The bi query to transform
    :param semantic_model: required for resolving kpis and filter refs and the model relationships
    :return: The generated sql query object
    :rtype: SQLQuery
    """
    # Define the from and join statements

    # 1. Extract the set of tables from the query
    tables = {d.table for d in bi.dimensions} | {m.table for m in bi.measures} | {f.table for f in bi.dimension_filters}
    # 2. Determine the table on the n most side of join - this is our from table
    # 3. Add in the joins based on the relationships in the semantic model

    # Define the select items
    # I think here we also define the group by clauses

    # Define the filters

    # Define the order by clauses

    # Define the final SQLQuery object
