from .logical_plan import LogicalPlan, PlanningResult
from datachain.data_model import DataModel, TableModel, Relationship
from datachain.resolver import ResolvedQuery
from ..errors import DataChainError

def generate_logical_plan(query: ResolvedQuery, data_model: DataModel) -> PlanningResult:
    """Responsible for finding the common tbale and the relationships between the tables in the query."""
    errors = []

    tables = get_tables_in_query(query, data_model)
    
    if len(tables) == 1:
        base_table = tables.pop()
        logical_plan = LogicalPlan(base_table=base_table, joins=[])
        return PlanningResult(success=True, logical_plan=logical_plan, errors=errors)
    
    graph = data_model.get_relationship_graph()
    base_table = find_base_table(tables, graph)

    if base_table is None:
        errors.append(DataChainError(
            stage="plan",
            message="No common table found among query tables",
            code="no_common_table",
        ))
        return PlanningResult(success=False, logical_plan=None, errors=errors)
    
    relationships = [find_join_path_to_base_table(base_table, table, graph) for table in tables if table != base_table]
    joins = []
    for rel in relationships:
        if rel not in joins:
            joins.append(rel)
    
    logical_plan = LogicalPlan(base_table=base_table, joins=joins)
    return PlanningResult(success=True, logical_plan=logical_plan, errors=errors)

def get_tables_in_query(
    query: ResolvedQuery,
    data_model: DataModel
) -> set[TableModel]:
    """Extract TableModels referenced in the resolved query."""

    tables: set[TableModel] = set()

    objects = (
        query.metrics
        + query.dimensions
        + query.filters
        + query.metric_filters
    )

    for obj in objects:
        expr = obj._cached_expr # resolve should have been called during resolution, so _cached_expr should be populated

        for relation in expr.relations:
            table_name = relation.get_name()
            table_model = data_model.get_table(table_name)

            if table_model is not None:
                tables.add(table_model)

    return tables

def find_base_table(tables: set[TableModel], graph: dict[TableModel, list[Relationship]]) -> TableModel | None:
    """Find a common base table that can join to all tables in the query."""
    reachability = {table: bfs_distances(table, graph) for table in tables}
    common = set.intersection(*[set(dist.keys()) for dist in reachability.values()])

    if not common:
        return None
    
    if len(common) == 1:
        return common.pop()
    
    # If there are multiple common tables, we can choose the one with the lowest total distance to all tables in the query
    return min(common, key=lambda table: sum(reachability[src].get(table, float('inf')) for src in tables))

def bfs_distances(start: TableModel, graph: dict[TableModel, list[Relationship]]) -> dict[TableModel, int]:
    """Perform BFS to find shortest distances from start to all reachable nodes."""
    visited = {start: 0}
    queue = [start]

    while queue:
        current = queue.pop(0)
        current_distance = visited[current]

        for neighbor in graph.get(current, []):
            if neighbor.right not in visited:
                visited[neighbor.right] = current_distance + 1
                queue.append(neighbor.right)

    return visited

def find_join_path_to_base_table(base_table: TableModel, table,  graph: dict[TableModel, list[Relationship]], visited=None) -> list[Relationship]:
    """DFS to find the join path from a table to the base table."""
    if visited is None:
        visited = set()

    if table == base_table:
        return []
    
    visited.add(table)

    for rel in graph.get(table, []):
        if rel.right in visited:
            continue
        
        path = find_join_path_to_base_table(base_table, rel.right, graph, visited)
        if path is not None:
            return [rel] + path

    return None
