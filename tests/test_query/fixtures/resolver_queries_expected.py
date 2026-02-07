from src.datachain.query.models import (
    BIQuery,
    BIDimension,
    BIMeasure,
    BIFilter,
    BIOrderBy,

    ResolvedBIQuery,
    ResolvedBIDimension,
    ResolvedBIDimensionTimeGrain,
    ResolvedOrderByMeasure,
    ResolvedOrderByDimension,
    ResolvedBIMeasureFilter,
    ResolvedBIFilter,

    Aggregation,
    Comparator,
    TimeGrain
)

kpi_and_resolved_measure = (
    BIQuery(
        kpi_refs=["kpi_total_revenue"],
    ),
    ResolvedBIQuery(
        measures=[
            BIMeasure(
                name="kpi_total_revenue",
                table="Sales",
                column="revenue",
                aggregation=Aggregation.SUM
            )
        ]
    )
)

semantic_filter_and_dimension_filter = (
    BIQuery(
        filter_refs=["filter_active_customers"]
    ),
    ResolvedBIQuery(
        dimension_filters=[
            ResolvedBIFilter(
                table="Customer",
                column="is_active",
                comparator=Comparator.EQUAL,
                value=True
            )
        ]
    )
)

semantic_filter_and_measure_filter = (
    BIQuery(
        filter_refs=["filter_high_revenue"]
    ),
    ResolvedBIQuery(
        measure_filters=[
            ResolvedBIMeasureFilter(
                measure=BIMeasure(
                    name="kpi_total_revenue",
                    table="Sales",
                    column="revenue",
                    aggregation=Aggregation.SUM
                ),
                comparator=Comparator.GREATER_THAN,
                value=10_000
            )
        ]
    )
)

measure_filter_and_resolved_measure_filter = (
    BIQuery(
        measures = [
            BIMeasure(
                name="total_revenue",
                table="Sales",
                column="revenue",
                aggregation=Aggregation.SUM
            )
        ],
        measure_filters=[
            BIFilter(
                field="total_revenue",
                comparator=Comparator.GREATER_THAN,
                value=10_000
            )
        ]
    ),
    ResolvedBIQuery(
        measures=[
            BIMeasure(
                name="total_revenue",
                table="Sales",
                column="revenue",
                aggregation=Aggregation.SUM
            )
        ],
        measure_filters=[
            ResolvedBIMeasureFilter(
                measure=BIMeasure(
                    name="total_revenue",
                    table="Sales",
                    column="revenue",
                    aggregation=Aggregation.SUM
                ),
                comparator=Comparator.GREATER_THAN,
                value=10_000
            )
        ]
    )
)

dimension_and_resolved_dimension_filter = (
    BIQuery(
        dimension_filters=[
            BIFilter(
                field="Customer.customer_id",
                comparator=Comparator.EQUAL,
                value="A123"
            )
        ]
    ),
    ResolvedBIQuery(
        dimension_filters=[
            ResolvedBIFilter(
                table="Customer",
                column="customer_id",
                comparator=Comparator.EQUAL,
                value="A123"
            ),
        ]
    )
)