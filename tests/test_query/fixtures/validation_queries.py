from src.datachain.query.models import (
    BIQuery,
    BIDimension,
    BIMeasure,
    BIFilter,
    BIOrderBy,

    Aggregation,
    Comparator,
    TimeGrain
)

# Structural Tests

no_selects_has_filters = BIQuery(
    filter_refs=['filter_active_customers'],
    dimension_filters=[
        BIFilter(field="Customer.customer_id", comparator=Comparator.EQUAL, value="A123")
        ]
)

no_selects_has_orderby = BIQuery(
    order_by=[
        BIOrderBy(
            field="Sales.revenue"
        )
    ]
)

has_selects = BIQuery(
    dimensions=[
        BIDimension(table="Customer", column="customer_id")
    ]
)

# Reference Tests
invalid_kpi_reference = BIQuery(
    kpi_refs=["kpi_total_costs"]
)

invalid_filter_ref = BIQuery(
    filter_refs=["filter_favorite_products"]
)

invalid_dimension_table = BIQuery(
    dimensions=[
        BIDimension(
            table="Region",
            column="area_code"
        )
    ]
)

invalid_dimension_column = BIQuery(
    dimensions=[
        BIDimension(
            table="Customer",
            column="name"
        )
    ]
)

invalid_timegrain = BIQuery(
    dimensions=[
        BIDimension(
            table="Customer",
            column="customer_id",
            time_grain="DAY"
        )
    ]
)

invalid_measure = BIQuery(
    measures=[
        BIMeasure(
            name="count_customer_name",
            table="Customer",
            column="name",
            aggregation=Aggregation.COUNT
        )
    ]
)

invalid_dimension_filter = BIQuery(
    dimension_filters=[
        BIFilter(
            field="Customer.name",
            comparator=Comparator.EQUAL,
            value="A123"
        )
    ]
)

valid_query = BIQuery(
    dimensions=[
        BIDimension(
            table="Customer",
            column="country"
        ),
        BIDimension(
            table="Sales",
            column="date_id",
            time_grain=TimeGrain.DAY
        )
    ],
    kpi_refs=["kpi_total_revenue"],
    filter_refs=["filter_high_revenue"]
)