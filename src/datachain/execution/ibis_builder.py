import ibis.expr.types as ir
from datachain.planner import LogicalPlan
from resolver import ResolvedQuery

def build_ibis_expression(logical_plan: LogicalPlan, query: ResolvedQuery) -> ir.Expr:
    """Build an Ibis expression from the logical plan and the resolved query."""
    # Start with the base table
    expr = logical_plan.base_table.ibis()

    # Apply joins
    for join in logical_plan.joins:
        right_expr = join.right.ibis()
        if join.left == logical_plan.base_table:
            left_expr = expr
        else:
            left_expr = join.left.ibis()
        
        # All joins are left joins
        expr = left_expr.join(right_expr, on=join.on(), how="left")

    # Apply filters, metrics, and dimensions from the query
    # This part is simplified and would need to be expanded to handle all aspects of the query
    for filter in query.filters:
        expr = expr.filter(filter._cached_expr)
    
    for metric in query.metrics:
        expr = expr.mutate(**{metric.name: metric._cached_expr})
    
    for dimension in query.dimensions:
        expr = expr.mutate(**{dimension.name: dimension._cached_expr})

    return expr