from .models import SemanticModel, BIQuery, ResolvedBIQuery
from .models import DataType
from collections import deque
from .orchestrator import QueryError, QueryContext

class QueryValidator():
    @staticmethod
    def validate_structure(bi_query: BIQuery, ctx: QueryContext) -> list[QueryError]:
        """Validates the basic structure of the biquery and updates the ctx trace"""
        errors: list[QueryError] = []
        
        ctx.trace.append("validating structure of bi query")

        # Must select something
        if not any([bi_query.dimensions, bi_query.measures, bi_query.kpi_refs]):
            errors.append(
                QueryError(
                    stage="structure_validation",
                    msg="you must select at least one dimensions, measure or kpi"
                )
            )
        
        # Only one time grain allowed
        if len([d for d in bi_query.dimensions if d.time_grain]) > 1:
            errors.append(
                QueryError(
                    stage="structure_validation",
                    msg="only one time grain dimension is allowed per query",
                )
            )

        return errors
        
    @staticmethod
    def validate_references(bi_query: BIQuery, semantic_model: SemanticModel, ctx: QueryContext) -> list[QueryError]:
        """
        Validates that all references in a BIQuery exist in the semantic model,
        including dimensions, measures, filters, KPIs, and ensures time-grain dimensions
        are of type DATE. Updates ctx.trace along the way.
        """
        errors: list[QueryError] = []
        ctx.trace.append("Validating semantic references against the semantic model")

        # 1️⃣ KPI references
        for kpi_name in bi_query.kpi_refs:
            ctx.trace.append(f"Checking KPI reference: {kpi_name}")
            if semantic_model.get_kpi(kpi_name) is None:
                errors.append(QueryError(
                    stage="reference_validation",
                    msg=f"KPI '{kpi_name}' does not exist in the semantic model"
                ))

        # 2️⃣ Filter references
        for filter_name in bi_query.filter_refs:
            ctx.trace.append(f"Checking filter reference: {filter_name}")
            if semantic_model.get_filter(filter_name) is None:
                errors.append(QueryError(
                    stage="reference_validation",
                    msg=f"Filter '{filter_name}' does not exist in the semantic model"
                ))

        # 3️⃣ Dimensions
        for dim in bi_query.dimensions:
            ctx.trace.append(f"Checking dimension: {dim.ref}")
            if not semantic_model.field_exists(dim.table, dim.column):
                errors.append(QueryError(
                    stage="reference_validation",
                    msg=f"Dimension '{dim.ref}' references unknown table/column in semantic model"
                ))
            # Time-grain dimensions must be DATE
            elif dim.time_grain is not None and not semantic_model.is_correct_type(dim.table, dim.column, DataType.DATE):
                errors.append(QueryError(
                    stage="reference_validation",
                    msg=f"Time-grain dimension '{dim.ref}' must be of type DATE"
                ))

        # 4️⃣ Measures
        for measure in bi_query.measures:
            ctx.trace.append(f"Checking measure: {measure.name} ({measure.table}.{measure.column})")
            if not semantic_model.field_exists(measure.table, measure.column):
                errors.append(QueryError(
                    stage="reference_validation",
                    msg=f"Measure '{measure.name}' references unknown table/column in semantic model"
                ))

        # 5️⃣ Dimension filters
        for f in bi_query.dimension_filters:
            ctx.trace.append(f"Checking dimension filter field: {f.field}")
            if not f.table or not f.column:
                errors.append(QueryError(
                    stage="reference_validation",
                    msg=f"Dimension filter '{f.field}' is not formatted as table.column"
                ))
                continue
            if not semantic_model.field_exists(f.table, f.column):
                errors.append(QueryError(
                    stage="reference_validation",
                    msg=f"Dimension filter '{f.field}' references unknown table/column in semantic model"
                ))

        return errors

    @staticmethod
    def validate_join_path(semantic_model: SemanticModel, ctx: QueryContext) -> list[QueryError]:
        """Validates the resolved query join path and updates the context with joins and common table"""
        errors: list[QueryError] = []

        ctx.trace.append("Validating join path")

        # Build the relationship graph
        graph = semantic_model.get_relationship_graph()
        if not graph:
            ctx.trace.append("No relationship graph defined")
            if len(ctx.tables) > 1:
                errors.append(QueryError(
                    stage="join_path_validation",
                    msg="No relationship graph is defined but there is more than 1 table in the query",
                    details="Cannot resolve joins; stop query execution"
                ))
            elif ctx.tables:
                ctx.common_table = next(iter(ctx.tables))
                ctx.trace.append(f"Single table in query, setting common_table to {ctx.common_table}")
            return errors

        ctx.trace.append(f"Relationship graph loaded with tables: {list(graph.keys())}")
        
        # BFS from each table in the query to all reachable tables
        reachability = {table: bfs_distances(table, graph) for table in ctx.tables}
        ctx.trace.append(f"Computed reachability for tables: {reachability}")

        # Find intersection of reachable nodes → common tables
        common = set.intersection(*[set(dist.keys()) for dist in reachability.values()])

        if not common:
            errors.append(QueryError(
                stage="join_path_validation",
                msg="The set of tables in the query do not have a common table",
                details="The data model does not support this combination of tables"
            ))
            ctx.trace.append("No common table found among query tables")
            return errors

        if len(common) == 1:
            ctx.common_table = common.pop()
            ctx.trace.append(f"Single common table found: {ctx.common_table}")
            return errors

        # Pick the common table that minimizes total distance from all query tables
        ctx.common_table = min(
            common,
            key=lambda t: sum(reachability[src][t] for src in ctx.tables)
        )
        ctx.trace.append(f"Multiple common tables found, selected {ctx.common_table} as optimal common table")

        return errors


def find_common_table(biquery: BIQuery, semantic_model: SemanticModel) -> str | None:
    graph = semantic_model.get_relationship_graph()
    if not graph:
        return None

    # All tables used in query
    query_tables = {
        d.table for d in biquery.dimensions
    } | {
        m.table for m in biquery.measures
    } | {
        f.table for f in biquery.dimension_filters
    } | {
        semantic_model.get_kpi(k).expression.table for k in biquery.kpi_refs
    }

    # For each table, compute shortest distance to all others
    reachability = {
        table: bfs_distances(table, graph)
        for table in query_tables
    }

    # Tables reachable from *every* query table
    common = set.intersection(
        *[set(dist.keys()) for dist in reachability.values()]
    )

    if not common:
        return None

    if len(common) == 1:
        return common.pop()

    # Pick table with smallest total join distance
    return min(
        common,
        key=lambda t: sum(reachability[src][t] for src in query_tables)
    )


def bfs_distances(start: str, graph: dict[str, list[str]]) -> dict[str, int]:
    """Return shortest join distance from start table to all reachable tables"""
    distances = {start: 0}
    queue = deque([start])

    while queue:
        current = queue.popleft()

        for neighbor in graph.get(current, []):
            if neighbor not in distances:
                distances[neighbor] = distances[current] + 1
                queue.append(neighbor)

    return distances
