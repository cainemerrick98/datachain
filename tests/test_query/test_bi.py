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