from src.datachain.query.models import (
    ResolvedBIQuery,
    ResolvedBIDimension,
    ResolvedBIDimensionTimeGrain,
    ResolvedBIFilter,
    ResolvedOrderByDimension,
    ResolvedOrderByMeasure,
    ResolvedBIMeasureFilter,
    BIMeasure,
    ChangeWindow,

    Aggregation,
    TimeGrain
)

requires_cte = ResolvedBIQuery(
    time_grained_dimensions=[
        ResolvedBIDimensionTimeGrain(
            time_grain=TimeGrain.WEEK,
            table="Sales",
            column="date_id"
        )
    ],
    measures=[
        BIMeasure(
            name="change_in_total_revenue",
            table="Sales",
            column="revenue",
            aggregation=Aggregation.SUM,
            window=ChangeWindow(
                period=1,
                mode="ABSOLUTE"
            )
        )
    ]
)


simple_no_window = ResolvedBIQuery(
    dimensions=[
        ResolvedBIDimension(table="Sales", column="sale_id")
    ],
    measures=[
        BIMeasure(
            name="total_revenue",
            table="Sales",
            column="revenue",
            aggregation=Aggregation.SUM,
        )
    ]
)


no_measures = ResolvedBIQuery(
    dimensions=[
        ResolvedBIDimension(table="Sales", column="sale_id")
    ],
    measures=[]
)


multi_table = ResolvedBIQuery(
    dimensions=[
        ResolvedBIDimension(table="Product", column="product_id"),
        ResolvedBIDimension(table="Customer", column="customer_id"),
    ],
    measures=[
        BIMeasure(
            name="total_revenue",
            table="Sales",
            column="revenue",
            aggregation=Aggregation.SUM,
        )
    ]
)