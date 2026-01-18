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
from .validators import find_common_table


def transform_bi_query_to_sql_query(biquery: BIQuery, semantic_model: SemanticModel) -> SQLQuery:
    """
    Sets up the SQLQuery object based on the BIQuery and SemanticModel provided.
    The BIQuery is already a validated query.
    
    :param bi: The bi query to transform
    :param semantic_model: required for resolving kpis and filter refs and the model relationships
    :return: The generated sql query object
    :rtype: SQLQuery
    """
    # Define the from and join statements
    common_table = find_common_table(biquery, semantic_model) # this is my from statement

    # 3. Add in the joins based on the relationships in the semantic model

    # Define the select items
    # I think here we also define the group by clauses

    # Define the filters

    # Define the order by clauses

    # Define the final SQLQuery object

def create_join_clauses(biquery: BIQuery, semantic_model: SemanticModel, common_table: str) -> list[Join]:
    # TODO: refactor this into a method on the bi as this code is also used in validators
    query_tables = {
        d.table for d in biquery.dimensions
    } | {
        m.table for m in biquery.measures
    } | {
        f.table for f in biquery.dimension_filters
    } | {
        semantic_model.get_kpi(k).expression.table for k in biquery.kpi_refs
    }

    joins = []
    # We already know from validation that all tables can join in to the common
    for table in query_tables:
        if table == common_table:
            continue

        else:
            #TODO: relationships in semantic model are not ordered in anyway
            # We do not want to search the relationship graph again for the join path
            # We need to think about this
            for relationship in semantic_model.relationships:
                ...
                joins.append(
                    Join(
                        table=
                    )
                )



