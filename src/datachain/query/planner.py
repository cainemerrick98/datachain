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