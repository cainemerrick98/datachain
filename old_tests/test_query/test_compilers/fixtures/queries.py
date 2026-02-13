from src.datachain.query.models import (
    SQLQuery,
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
    Aggregation
)

simplest_sql_query = SQLQuery(
    from_="Customer",
    columns=[
        SelectItem(
            alias="ID",
            expression=QueryColumn(table="Customer", name="customer_id")
        )
    ]
)

simple_agg_and_groupby_sql_query = SQLQuery(
    from_="Sales",
    columns=[
        SelectItem(
            alias="ID",
            expression=QueryColumn(table="Sales", name="customer_id")
        ),
        SelectItem(
            alias="total_revenue",
            expression=SQLMeasure(
                table="Sales",
                column="revenue",
                aggregation=Aggregation.SUM
            )
        )
    ],
    group_by=[
        GroupBy(
            table="Sales",
            column=QueryColumn(table="Sales", name="customer_id")
        )
    ]
)