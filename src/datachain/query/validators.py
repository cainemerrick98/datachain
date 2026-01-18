from .models import SemanticModel, BIQuery
from .models import DataType
from collections import defaultdict

# TODO: split this up into multiple smaller validators
def validate_biquery_agaisnt_semantic_model(biquery: BIQuery, semantic_model: SemanticModel) -> tuple[bool, list[dict]]:
    """
    Responsible for checking if a biquery is valid given a semantic model 
    and if not collecting the errors to pass to pass to the LLM
    """

    is_valid = True

    valid_fields = set()
    valid_fields |= {f"{table.name}.{column.name}" for table in semantic_model.tables for column in table.columns}
    valid_fields |= {kpi.name for kpi in semantic_model.kpis}
    valid_fields |= {_filter.name for _filter in semantic_model.filters}
    
    # Validate that all referenced fields are in the semantic model
    errors = []
    #TODO update filters in referenced fields set
    referenced_fields = {d.ref for d in biquery.dimensions} | {k for k in biquery.kpi_refs} | {m.ref for m in biquery.measures} | {f for f in biquery.filter_refs} | {f.field for f in biquery.dimension_filters}
    for field in referenced_fields:
        if field not in valid_fields:
            is_valid = False
            # TODO: add more informative error e.g. where is the invalid field in the biquery
            errors.append({
                "type": "invalid_field",
                "msg": f"field: {field} is not in the semantic model"
            })

    # Validate that if time grain is applied, the dimension is a date type
    date_columns = {f"{table.name}.{column.name}" for table in semantic_model.tables for column in table.columns if column.type == DataType.DATE}
    for dimension in biquery.dimensions:
        if dimension.time_grain is not None:
            if dimension.ref not in date_columns:
                is_valid = False
                errors.append({
                    "type": "invalid_time_grain",
                    "msg": f"dimension: {dimension.ref} has time grain applied but is not a date type"
                })

    common_table = find_common_table(biquery, semantic_model)
    if not common_table:
        is_valid = False
        errors.append({
            "type": "invalid_join_path",
            "msg": f"no valid join path between tables in the biquery"
        })
        
    return is_valid, errors

from collections import defaultdict, deque


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
