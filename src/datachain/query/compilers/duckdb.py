from .base import BaseSQLCompiler
from ..models import SQLQuery
from ..models import (
    SelectItem,
    QueryColumn,
    TimeGrainedQueryColumn,
    SQLMeasure,
    BinaryMetric,
    WindowSpec,
    SQLChangeWindow,
    SQLMovingAverageWindow,
    Comparison,
    And,
    Or,
    Not,
    ColumnComparison,
    Join,
    GroupBy,
    OrderBy,
    HavingComparison,
)


class DuckDbCompiler(BaseSQLCompiler):
    @staticmethod
    def compile(query: SQLQuery) -> str:
        
        def render_identifier(table: str, column: str) -> str:
            return f"{table}.{column}"

        def render_metric(expr) -> str:
            if isinstance(expr, QueryColumn):
                return render_identifier(expr.table, expr.name)
            if isinstance(expr, TimeGrainedQueryColumn):
                # Use date_trunc style for time grain where appropriate
                return f"{expr.time_grain}({render_identifier(expr.table, expr.name)})"
            if isinstance(expr, SQLMeasure):
                agg = expr.aggregation.value if hasattr(expr.aggregation, 'value') else str(expr.aggregation)
                return f"{agg}({render_identifier(expr.table, expr.column)})"
            if isinstance(expr, BinaryMetric):
                left = render_metric(expr.left)
                op = expr.arithmetic.value
                right = render_metric(expr.right)
                return f"({left} {op} {right})"
            if isinstance(expr, WindowSpec):
                field = expr.field
                partition = ''
                order = ''
                if expr.partition_by:
                    parts = [render_metric(p) for p in expr.partition_by]
                    partition = 'PARTITION BY ' + ', '.join(parts)
                if expr.order_by:
                    parts = []
                    for o in expr.order_by:
                        if isinstance(o, OrderBy):
                            parts.append(f"{render_metric(o.column)} {o.sorting.value}")
                        else:
                            parts.append(render_metric(o))
                    order = 'ORDER BY ' + ', '.join(parts)

                over = ' '.join([s for s in (partition, order) if s])

                # Support simple change and moving average windows
                w = expr.window
                if isinstance(w, SQLChangeWindow):
                    # ABSOLUTE: field - lag(field, period)
                    # PERCENTAGE: (field - lag(field, period)) / NULLIF(lag(field, period),0) * 100
                    lag = f"LAG({field}, {w.period}) OVER ({over})"
                    if w.mode == 'ABSOLUTE':
                        return f"({field} - {lag})"
                    return f"(({field} - {lag}) / NULLIF({lag},0) * 100)"
                if isinstance(w, SQLMovingAverageWindow):
                    # Simple moving average using window frame of preceding (period-1) rows
                    return f"AVG({field}) OVER ({over} ROWS BETWEEN {w.period-1} PRECEDING AND CURRENT ROW)"

                return f"{field} OVER ({over})"

            # Fallback
            return str(expr)

        def render_value(v):
            if v is None:
                return 'NULL'
            if isinstance(v, str):
                return f"'{v}'"
            if isinstance(v, list):
                items = ', '.join(render_value(x) for x in v)
                return f"({items})"
            return str(v)

        def render_pred(pred) -> str:
            if pred is None:
                return ''
            if isinstance(pred, Comparison):
                comp = pred.comparator.value if hasattr(pred.comparator, 'value') else str(pred.comparator)
                if comp in ('IS NULL', 'IS NOT NULL'):
                    return f"{render_identifier(pred.table, pred.column)} {comp}"
                if comp in ('IN', 'NOT IN'):
                    return f"{render_identifier(pred.table, pred.column)} {comp} {render_value(pred.value)}"
                return f"{render_identifier(pred.table, pred.column)} {comp} {render_value(pred.value)}"
            if isinstance(pred, ColumnComparison):
                comp = pred.comparator.value if hasattr(pred.comparator, 'value') else str(pred.comparator)
                return f"{pred.left} {comp} {pred.right}"
            if isinstance(pred, And):
                parts = [render_pred(p) for p in pred.predicates]
                return '(' + ' AND '.join(parts) + ')'
            if isinstance(pred, Or):
                parts = [render_pred(p) for p in pred.predicates]
                return '(' + ' OR '.join(parts) + ')'
            if isinstance(pred, Not):
                return f"NOT ({render_pred(pred.predicate)})"

            return str(pred)

        def render_join(j: Join) -> str:
            conditions = []
            for l, r in zip(j.left_keys, j.right_keys):
                conditions.append(f"{j.left_table}.{l} = {j.right_table}.{r}")
            on = ' AND '.join(conditions)
            return f"LEFT JOIN {j.right_table} ON {on}"

        def render_group_by(group_by: list[GroupBy]) -> str:
            if not group_by:
                return ''
            parts = []
            for g in group_by:
                if isinstance(g.column, TimeGrainedQueryColumn):
                    parts.append(render_metric(g.column))
                else:
                    parts.append(render_identifier(g.table, g.column.name if hasattr(g.column, 'name') else g.column))
            return 'GROUP BY ' + ', '.join(parts)

        def render_order_by(order_by: list[OrderBy]) -> str:
            if not order_by:
                return ''
            parts = []
            for o in order_by:
                col = o.column
                if isinstance(col, (QueryColumn, TimeGrainedQueryColumn, SQLMeasure, BinaryMetric, WindowSpec)):
                    parts.append(f"{render_metric(col)} {o.sorting.value}")
                else:
                    parts.append(f"{col} {o.sorting.value}")
            return 'ORDER BY ' + ', '.join(parts)

        def render_select_columns(cols: list[SelectItem]) -> str:
            parts = []
            for c in cols:
                expr = c.expression
                sql = render_metric(expr)
                if c.alias:
                    parts.append(f"{sql} AS {c.alias}")
                else:
                    parts.append(sql)
            return ', '.join(parts)

        # Internal compile that can produce either a subquery body or full WITH-wrapped query
        def _compile(q: SQLQuery, as_subquery: bool = False) -> str:
            select_clause = 'SELECT ' + (render_select_columns(q.columns) if q.columns else '*')
            # FROM
            if isinstance(q.from_, SQLQuery):
                # compile inner as subquery for CTE
                inner = _compile(q.from_, as_subquery=True)
                from_clause = f"FROM ({inner}) AS cte"
            else:
                from_clause = f"FROM {q.from_}"

            join_clause = ''
            if q.joins:
                join_clause = ' ' + ' '.join(render_join(j) for j in q.joins)

            where_clause = ''
            if q.filters:
                where_clause = 'WHERE ' + render_pred(q.filters)

            group_clause = render_group_by(q.group_by) if getattr(q, 'group_by', None) else ''

            having_clause = ''
            if q.having:
                if isinstance(q.having, HavingComparison):
                    having_clause = 'HAVING ' + f"{render_metric(q.having.metric)} {q.having.comparator.value} {render_value(q.having.value)}"
                else:
                    having_clause = 'HAVING ' + render_pred(q.having)

            order_clause = render_order_by(q.order_by) if getattr(q, 'order_by', None) else ''

            limit_clause = f"LIMIT {q.limit}" if getattr(q, 'limit', None) is not None else ''
            offset_clause = f"OFFSET {q.offset}" if getattr(q, 'offset', None) is not None else ''

            parts = [select_clause, from_clause]
            if join_clause:
                parts.append(join_clause)
            if where_clause:
                parts.append(where_clause)
            if group_clause:
                parts.append(group_clause)
            if having_clause:
                parts.append(having_clause)
            if order_clause:
                parts.append(order_clause)
            if limit_clause:
                parts.append(limit_clause)
            if offset_clause:
                parts.append(offset_clause)

            body = '\n'.join(parts)
            return body

        # If top-level from_ is SQLQuery we should render as CTE
        if isinstance(query.from_, SQLQuery):
            inner = _compile(query.from_, as_subquery=True)
            outer_select = 'SELECT ' + (render_select_columns(query.columns) if query.columns else '*')
            outer_from = 'FROM cte'
            outer_order = render_order_by(query.order_by) if getattr(query, 'order_by', None) else ''
            outer_limit = f"LIMIT {query.limit}" if getattr(query, 'limit', None) is not None else ''

            body_parts = [f"WITH cte AS (", inner, ")", outer_select, outer_from]
            if outer_order:
                body_parts.append(outer_order)
            if outer_limit:
                body_parts.append(outer_limit)

            return '\n'.join(body_parts)

        return _compile(query)
