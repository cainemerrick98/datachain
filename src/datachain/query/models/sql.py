from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional, Union, List, Literal
from .enums import Aggregation, Comparator, Sorting

class Arithmetic(Enum):
    """Arithmetic operators for calculated metrics"""
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "%"


class JoinType(Enum):
    """SQL join types for combining tables"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


# Filtering

ComparisonValue = Union[int, float, str, List[int], List[str], None]
Predicates = Union["And", "Comparison", "Or", "Not", "ColumnComparison"]

class Comparison(BaseModel):
    """A single comparison condition for filtering data (e.g., price > 100, region = 'West')"""
    column: str = Field(description="Column name to compare")
    comparator: Comparator = Field(description="Comparison operator (=, >, <, >=, <=, IN, LIKE, IS NULL, etc.)")
    value: Optional[ComparisonValue] = Field(
        None, 
        description="Value to compare against. Use None for IS NULL/IS NOT NULL operators. Use a list of values for IN/NOT IN operators. Use a string with % wildcards for LIKE operator (e.g., '%search%')."
    )


class ColumnComparison(BaseModel):
    """A single comparison condition between two columns (e.g. qty > price, first_name = name)"""
    left: str = Field(description="Column name to the left of the operator")
    comparator: Comparator = Field(description="Comparison operator (=, >, <, >=, <=, IN, LIKE, IS NULL, etc.)")
    right: str = Field(description="Column name to the right of the operator")


class And(BaseModel):
    """Combine multiple conditions with AND logic - all conditions must be true"""
    predicates: List[Predicates] = Field(
        min_length=1,
        description="List of conditions that must all be true. Can include Comparison, Or, Not, or FilterRef objects."
    )


class Or(BaseModel):
    """Combine multiple conditions with OR logic - any condition can be true"""
    predicates: List[Predicates] = Field(
        min_length=1,
        description="List of conditions where at least one must be true. Can include Comparison, And, Not, or FilterRef objects."
    )


class Not(BaseModel):
    """Negate a condition - makes true conditions false and vice versa"""
    predicate: Predicates = Field(
        description="The condition to negate. Can be a Comparison, And, Or, or FilterRef object."
    )


# Metrics

MetricExpr = Union["QueryColumn", "SQLMeasure", "BinaryMetric"]

class QueryColumn(BaseModel):
    """A reference to a raw column from the table without any aggregation"""
    name: str = Field(
        description="Name of the column from the table (e.g., 'product_name', 'region', 'date')"
    )


class SQLMeasure(BaseModel):
    """An aggregated metric using functions like SUM, COUNT, AVG, etc."""
    column: str = Field(
        description="Name of the column to aggregate (e.g., 'revenue', 'order_id', 'quantity')"
    )
    aggregation: Aggregation = Field(
        description="Aggregation function to apply: SUM for totals, AVG for averages, COUNT for counting rows, COUNT_DISTINCT for unique values, MIN/MAX for extremes, STDDEV/VARIANCE for statistical measures, MEDIAN for middle values"
    )


class BinaryMetric(BaseModel):
    """A calculated metric combining two metrics with arithmetic operations (e.g., revenue / orders = avg_order_value)"""
    left: MetricExpr = Field(
        description="Left side of the calculation. Can be a QueryColumn, Measure, or another BinaryMetric."
    )
    arithmetic: Arithmetic = Field(
        description="Arithmetic operation to perform: ADD (+), SUB (-), MUL (*), DIV (/), or MOD (%)"
    )
    right: MetricExpr = Field(
        description="Right side of the calculation. Can be a QueryColumn, Measure, or another BinaryMetric."
    )


class SelectItem(BaseModel):
    """A column or metric to include in the SELECT clause, with an optional alias"""
    alias: Optional[str] = Field(
        None,
        description="Optional alias name for the result column (e.g., 'total_revenue', 'avg_price'). If not provided, a default name will be generated."
    )
    expression: MetricExpr = Field(
        description="The expression to select: use QueryColumn for raw columns, Measure for aggregations (SUM, COUNT, AVG, etc.), or BinaryMetric for calculated metrics (revenue / orders)."
    )


# Joining
class Join(BaseModel):
    """
    A join operation to combine the primary table with another table.
    
    Joins allow you to combine data from multiple tables based on a relationship between them.
    For example, joining an orders table with a customers table to get customer details for each order.
    """
    table: str = Field(
        description="Name of the table to join with the primary table (e.g., 'customers', 'products'). This table will be combined with the main query table."
    )
    condition: Predicates = Field(
        description="Join condition that defines how rows from the two tables should be matched. Typically uses ColumnComparison to match columns between tables (e.g., orders.customer_id = customers.id). Use And for multiple join conditions."
    )
    type: JoinType = Field(
        default=JoinType.INNER,
        description="Type of join: INNER (only matching rows from both tables), LEFT (all rows from primary table, matching rows from joined table), RIGHT (matching rows from primary table, all rows from joined table), FULL (all rows from both tables), CROSS (cartesian product of both tables)"
    )

# Query

class GroupBy(BaseModel):
    """Specifies a column to group by when using aggregations"""
    column: QueryColumn = Field(
        description="Column to group by. When using aggregations like SUM or COUNT, all non-aggregated columns in the SELECT must be included in GROUP BY."
    )


class OrderBy(BaseModel):
    """Specifies sorting for query results"""
    column: Union[QueryColumn, MetricExpr] = Field(
        description="Column or metric to sort by. Can be a raw column, aggregated measure, or calculated metric."
    )
    sorting: Sorting = Field(
        default=Sorting.ASC,
        description="Sort direction: ASC for ascending (lowest to highest), DESC for descending (highest to lowest). Defaults to ASC."
    )


class SQLQuery(BaseModel):
    """
    A structured SQL query that prevents SQL injection and ensures valid queries.
    
    Use this to query database tables with filtering, aggregations, grouping, sorting, and joins.
    All queries are validated to ensure they are syntactically correct and safe to execute.
    
    Examples:
        Simple select with columns:
        Query(
            table_name="customers",
            columns=[SelectItem(expression=QueryColumn(kind="column", name="name"))]
        )
        
        Aggregation with filter:
        Query(
            table_name="sales",
            columns=[
                SelectItem(
                    alias="total_revenue",
                    expression=Measure(kind="measure", column="amount", aggregation=Aggregation.SUM)
                )
            ],
            filters=Comparison(column="date", comparator=Comparator.GREATER_THAN, value="2024-01-01")
        )
        
        Group by with multiple conditions:
        Query(
            table_name="orders",
            columns=[
                SelectItem(expression=QueryColumn(kind="column", name="region")),
                SelectItem(
                    alias="order_count",
                    expression=Measure(kind="measure", column="order_id", aggregation=Aggregation.COUNT)
                )
            ],
            filters=And(predicates=[
                Comparison(column="status", comparator=Comparator.EQUAL, value="completed"),
                Comparison(column="region", comparator=Comparator.IN, value=["North", "South"])
            ]),
            groupby=[GroupBy(column=QueryColumn(kind="column", name="region"))],
            orderby=[OrderBy(
                column=Measure(kind="measure", column="order_id", aggregation=Aggregation.COUNT),
                sorting=Sorting.DESC
            )],
            limit=10
        )
        
        Join with another table:
        Query(
            table_name="orders",
            columns=[
                SelectItem(expression=QueryColumn(kind="column", name="order_id")),
                SelectItem(expression=QueryColumn(kind="column", name="customer_name"))
            ],
            joins=[
                Join(
                    table="customers",
                    condition=Comparison(column="orders.customer_id", comparator=Comparator.EQUAL, value="customers.id"),
                    type=JoinType.LEFT
                )
            ]
        )
    """
    table_name: str = Field(
        description="Name of the primary database table to query (e.g., 'sales', 'customers', 'orders'). This is the main table that other tables may be joined to."
    )
    columns: List[SelectItem] = Field(
        description="Columns to select in the results. Use SelectItem with QueryColumn for raw columns, SelectItem with Measure for aggregations (SUM, COUNT, AVG, MIN, MAX, etc.), SelectItem with BinaryMetric for calculated metrics, or KpiRef to reference predefined metrics from the semantic model."
    )
    filters: Optional[Predicates] = Field(
        None,
        description="WHERE clause conditions to filter rows before grouping. Use Comparison for simple conditions (column = value), And to require all conditions, Or for any condition, Not to negate, or FilterRef to reference predefined filters from the semantic model."
    )
    joins: Optional[List[Join]] = Field(
        None,
        description="JOIN clauses to combine data from other tables. Specify the table to join, the join condition (how rows match), and the join type (INNER, LEFT, RIGHT, FULL, CROSS). Multiple joins are applied in the order specified."
    )
    groupby: Optional[List[GroupBy]] = Field(
        None,
        description="GROUP BY clause - required when using aggregations like SUM, COUNT, AVG, MIN, MAX. Include all non-aggregated columns from the SELECT clause here. For example, if selecting region and SUM(revenue), include region in groupby."
    )
    orderby: Optional[List[OrderBy]] = Field(
        None,
        description="ORDER BY clause to sort results. Specify one or more columns with sort direction (ASC/DESC). Can sort by raw columns, aggregated measures, or calculated metrics. Results are sorted by the first column, then by subsequent columns for ties."
    )
    limit: Optional[int] = Field(
        None,
        description="LIMIT clause to restrict the number of rows returned. Specify a positive integer to return only the first N rows after filtering, grouping, and sorting. Useful for getting top results or pagination."
    )
    offset: Optional[int] = Field(
        None,
        description="OFFSET clause to skip a specified number of rows before returning results. Used with LIMIT for pagination. For example, LIMIT 10 OFFSET 20 returns rows 21-30. OFFSET is applied after filtering, grouping, and sorting."
    )