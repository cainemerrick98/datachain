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
    BIMeasure,
    BIDimension,

    SemanticModel,
    KPI,
    Filter
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

    # Add in the joins based on the relationships in the semantic model
    joins = create_join_clauses(biquery, semantic_model, common_table)

    # Define the select items
    select_items = []
    for item in biquery.dimensions + biquery.kpi_refs + biquery.measures:
        if isinstance(item, str):
            item = semantic_model.get_kpi(item)
        select_items.append(create_select_item(item))

    # I think here we also define the group by clauses
    groupby = []
    if len(biquery.kpi_refs + biquery.measures) > 0:
        for dim in biquery.dimensions:
            groupby.append(
                GroupBy(
                    table=dim.table,
                    column=dim.column
                )
            )

    # Define the filters
    filters = []
    having = []
    for fil in biquery.dimension_filters:
        filters.append(
            
        )


    # Define the order by clauses

    # Define the final SQLQuery object


def create_join_clauses(
    biquery: BIQuery,
    semantic_model: SemanticModel,
    common_table: str
) -> list[Join]:

    query_tables = {
        d.table for d in biquery.dimensions
    } | {
        m.table for m in biquery.measures
    } | {
        f.table for f in biquery.dimension_filters
    } | {
        semantic_model.get_kpi(k).expression.table for k in biquery.kpi_refs
    }

    if len(query_tables) == 1:
        return []

    graph = semantic_model.get_relationship_graph()

    ordered_edges: list[tuple[str,str]] = []

    for table in query_tables:
        if table == common_table:
            continue

        path = find_join_path_to_common_table(
            table, graph, common_table
        )

        if path is None:
            raise ValueError(
                f"No join path from {table} to {common_table}"
            )

        for edge in path:
            if edge not in ordered_edges:
                ordered_edges.append(edge)

    sql_joins: list[Join] = []

    joined_tables = {common_table}

    for incoming, outgoing in reversed(ordered_edges):
        # ensure parent is already joined
        if outgoing in joined_tables:
            joined_tables.add(incoming)

            for rel in semantic_model.relationships:
                if (
                    rel.incoming == incoming
                    and rel.outgoing == outgoing
                ):
                    sql_joins.append(
                        Join(
                            left_table=incoming,
                            right_table=outgoing,
                            left_keys=rel.keys_incoming,
                            right_keys=rel.keys_outgoing
                        )
                    )


    return sql_joins


def find_join_path_to_common_table(
    table: str,
    graph: dict[str, list[str]],
    common_table: str,
    visited=None
) -> list[tuple[str, str]] | None:
    """
    DFS to find the path from table to common_table.
    Returns list of join tuples [(A,B), (B,C), ...] or None if no path.
    """

    if visited is None:
        visited = set()

    if table == common_table:
        return []

    visited.add(table)

    for neighbor in graph.get(table, []):
        if neighbor in visited:
            continue

        path = find_join_path_to_common_table(
            neighbor, graph, common_table, visited
        )

        if path is not None:
            return [(table, neighbor)] + path

    return None


def create_select_item(item: BIDimension | KPI | BIMeasure) -> SelectItem:
    if isinstance(item, BIDimension):
        return SelectItem(
            alias=item.column,
            expression=QueryColumn(
                table=item.table,
                name=item.column
            )
        )
    #TODO here we are not supporting binary metrics!
    if isinstance(item, KPI):
        return SelectItem(
            alias=item.name,
            expression=SQLMeasure(
                table=item.expression.table,
                column=item.expression.column,
                aggregation=item.expression.aggregation,
            )
        )

    if isinstance(item, BIMeasure):
        return SelectItem(
            alias=item.name,
            expression=SQLMeasure(
                table=item.table,
                column=item.column,
                aggregation=item.aggregation,
                window=item.window
            )
        )
    
