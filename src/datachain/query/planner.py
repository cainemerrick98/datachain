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
    Predicates,
    HavingPredicates,

    BIQuery,
    BIMeasure,
    BIDimension,

    ResolvedBIQuery,
    ResolvedBIFilter,
    ResolvedBIMeasureFilter,
    

    SemanticModel,
    KPI,
    Filter
)
from .validator import find_common_table
from .orchestrator import QueryContext
from .models import ResolvedBIQuery

class QueryPlanner():

    def analyse_context(self, resolved_bi_query: ResolvedBIQuery, ctx: QueryContext, semantic_model: SemanticModel):
        """Check if CTE is required and compute joins"""
        
        ctx.trace.append("Checking if CTE is required due to window functions")
        if any([m.window for m in resolved_bi_query.measures]):
            ctx.requires_cte = True
            ctx.trace.append("CTE required because at least one measure has a window function")

        ctx.trace.append("Resolving join paths for tables")
        if len(ctx.tables) <= 1:
            ctx.trace.append("Single table; no joins required")
            return

        graph = semantic_model.get_relationship_graph()
        if not graph:
            ctx.trace.append("No relationship graph defined; cannot determine joins")
            ctx.requires_cte = True
            return

        for table in ctx.tables:
            if table == ctx.common_table:
                continue

            ctx.trace.append(f"Finding join path from table '{table}' to common table '{ctx.common_table}'")
            path = self._find_join_path_to_common_table(table, ctx.common_table, graph)
            if path is None:
                ctx.trace.append(f"No join path found from '{table}' to '{ctx.common_table}'; may require subquery")
                ctx.requires_cte = True
                continue

            for edge in path:
                if edge not in ctx.joins:
                    ctx.joins.append(edge)
                    ctx.trace.append(f"Added join: {edge[0]} -> {edge[1]}")
    
    def _find_join_path_to_common_table(self, table: str, common_table: str, graph: dict, visited=None) -> list[tuple[str, str]]:
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

            path = self._find_join_path_to_common_table(
                neighbor, graph, common_table, visited
            )

            if path is not None:
                return [(table, neighbor)] + path

        return None


    def plan(
        self,
        resolved_query: ResolvedBIQuery,
        ctx: QueryContext,
        semantic_model: SemanticModel,
    ) -> SQLQuery:
        """
        Build the final SQLQuery.
        If ctx.requires_cte is True:
          - CTE does ALL aggregation, windows, grouping, filters, having
          - Final query selects from CTE and applies ordering + limit
        """

        ctx.trace.append("planning SQL query")

        # ----------------------------
        # Common SELECT items
        # ----------------------------
        dimension_columns = [
            SelectItem(
                alias=None,
                expression=QueryColumn(table=d.table, name=d.column),
            )
            for d in resolved_query.dimensions
        ]

        measure_columns = [
            SelectItem(
                alias=m.name,
                expression=m,
            )
            for m in resolved_query.measures
        ]

        select_items = dimension_columns + measure_columns

        joins = map_joins(ctx, semantic_model)

        where_filters = map_dimension_filters(resolved_query.dimension_filters)
        having_filters = map_having_filters(resolved_query.measure_filters)

        group_by = [
            GroupBy(
                table=d.table,
                column=QueryColumn(table=d.table, name=d.column),
            )
            for d in resolved_query.dimensions
        ] or None

        # ============================
        # CASE 1: CTE REQUIRED
        # ============================
        if ctx.requires_cte:
            ctx.trace.append("building CTE for aggregations and window functions")

            cte_query = SQLQuery(
                from_=ctx.common_table,
                columns=select_items,
                filters=where_filters,
                having=having_filters,
                joins=joins or None,
                group_by=group_by,
                order_by=None,
                limit=None,
                offset=None,
            )

            ctx.trace.append("building outer query selecting from CTE")

            outer_columns = [
                SelectItem(
                    alias=item.alias,
                    expression=QueryColumn(
                        table="cte",
                        name=item.alias or (
                            item.expression.name
                            if hasattr(item.expression, "name")
                            else None
                        ),
                    ),
                )
                for item in select_items
            ]

            return SQLQuery(
                from_=cte_query,
                columns=outer_columns,
                filters=None,
                having=None,
                joins=None,
                group_by=None,
                order_by=resolved_query.order_by,
                limit=resolved_query.limit,
                offset=None,
            )

        # ============================
        # CASE 2: NO CTE REQUIRED
        # ============================
        ctx.trace.append("building single-stage query (no CTE)")

        return SQLQuery(
            from_=ctx.common_table,
            columns=select_items,
            filters=where_filters,
            having=having_filters,
            joins=joins or None,
            group_by=group_by,
            order_by=resolved_query.order_by,
            limit=resolved_query.limit,
            offset=None,
        )


def map_dimension_filters(filters: list[ResolvedBIFilter]) -> Predicates | None:
    if not filters:
        return None

    preds = [
        Comparison(
            table=f.table,
            column=f.column,
            comparator=f.comparator,
            value=f.value,
        )
        for f in filters
    ]

    return preds[0] if len(preds) == 1 else And(predicates=preds)


def map_having_filters(filters: list[ResolvedBIMeasureFilter]) -> HavingPredicates | None:
    if not filters:
        return None

    preds = [
        HavingComparison(
            metric=f.measure,
            comparator=f.comparator,
            value=f.value,
        )
        for f in filters
    ]

    return preds[0] if len(preds) == 1 else And(predicates=preds)


def map_joins(ctx: QueryContext, semantic_model: SemanticModel) -> list[Join]:
    joins: list[Join] = []

    for left, right in reversed(ctx.joins):
        #TODO this is inefficient 
        #Create a ds on the semantic model
        for rel in semantic_model.relationships:
            if (
                rel.incoming == left
                and rel.outgoing == right
            ):
                joins.append(
                    Join(
                        left_table=left,
                        right_table=right,
                        left_keys=rel.keys_incoming,
                        right_keys=rel.keys_outgoing
                    )
                )

    return joins


## --- OLD --- ##
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
    
