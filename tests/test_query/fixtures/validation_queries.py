from src.datachain.query.models import (
    BIQuery,
    BIDimension,
    BIMeasure,
    BIFilter,
    BIOrderBy,

    Aggregation,
    Comparator,
)

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