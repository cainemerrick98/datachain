"""
To test the validation logic on the BIQuery class
"""

import pytest
from src.datachain.query.models import (
    BIQuery,
    BIDimension,
    BIFilter,
    BIMeasure,
    BIOrderBy,
    ChangeWindow,
    Aggregation,
    Comparator,
    Sorting
)
from pydantic import ValidationError


def test_valid_query_simple():
    BIQuery(
        dimensions=[
            BIDimension(
                table="sales",
                column="customer"
            )
        ],
        measures=[
            BIMeasure(
                name="total_sales",
                table="sales",
                column="amount",
                aggregation=Aggregation.SUM
            )
        ],
        inline_filters=[
            BIFilter(
                field="sales.customer",
                comparator=Comparator.IN,
                value=['CustomerA', 'CustomerB']
            )
        ]
    )

def test_valid_query_filter_references_measure():
    BIQuery(
        dimensions=[
            BIDimension(
                table="sales",
                column="customer"
            )
        ],
        measures=[
            BIMeasure(
                name="total_sales",
                table="sales",
                column="amount",
                aggregation=Aggregation.SUM
            )
        ],
        inline_filters=[
            BIFilter(
                field="total_sales",
                comparator=Comparator.GREATER_THAN,
                value=1000
            )
        ]
    )

def test_invalid_empty_query():
    with pytest.raises(ValidationError):
        BIQuery()

def test_invalid_query_filter_has_no_value():
    with pytest.raises(ValidationError):
        BIQuery(
            dimensions=[
                BIDimension(
                    table="sales",
                    column="customer"
                )
            ],
            measures=[
                BIMeasure(
                    name="total_sales",
                    table="sales",
                    column="amount",
                    aggregation=Aggregation.SUM
                )
            ],
            inline_filters=[
                BIFilter(
                    field="sales.customer",
                    comparator=Comparator.IN,
                )
            ]
        )

def test_invalid_query_incorrect_orderby_field():
    with pytest.raises(ValidationError):
        BIQuery(
            dimensions=[
                BIDimension(
                    table="sales",
                    column="customer"
                )
            ],
            measures=[
                BIMeasure(
                    name="total_sales",
                    table="sales",
                    column="amount",
                    aggregation=Aggregation.SUM
                )
            ],
            order_by=[
                BIOrderBy(
                    field="sales.quantity",
                    sorting=Sorting.ASC
                )
            ]
        )

def test_invalid_query_multiple_time_grain_dimensions():
    with pytest.raises(ValidationError):
        BIQuery(
            dimensions=[
                BIDimension(
                    table="sales",
                    column="order_date",
                    time_grain="MONTH"
                ),
                BIDimension(
                    table="sales",
                    column="ship_date",
                    time_grain="DAY"
                )
            ],
            measures=[
                BIMeasure(
                    name="total_sales",
                    table="sales",
                    column="amount",
                    aggregation=Aggregation.SUM
                )
            ]
        )

def test_valid_query_single_time_grain_dimension():
    BIQuery(
        dimensions=[
            BIDimension(
                table="sales",
                column="order_date",
                time_grain="MONTH"
            )
        ],
        measures=[
            BIMeasure(
                name="total_sales",
                table="sales",
                column="amount",
                aggregation=Aggregation.SUM
            )
        ]
    )

def test_invalid_query_window_function_without_time_grain_measure():
    with pytest.raises(ValidationError):
        BIQuery(
            dimensions=[
                BIDimension(
                    table="sales",
                    column="customer"
                )
            ],
            measures=[
                BIMeasure(
                    name="running_total_sales",
                    table="sales",
                    column="amount",
                    aggregation=Aggregation.SUM,
                    window=ChangeWindow(period=7, mode="ABSOLUTE")
                )
            ]
        )

def test_valid_query_window_function_with_time_grain_measure():
    BIQuery(
        dimensions=[
            BIDimension(
                table="sales",
                column="order_date",
                time_grain="DAY"
            )
        ],
        measures=[
            BIMeasure(
                name="running_total_sales",
                table="sales",
                column="amount",
                aggregation=Aggregation.SUM,
                window=ChangeWindow(period=7, mode="ABSOLUTE")
            )
        ]
    )