"""
To test the validation of a BIQuery agaisnt a Semantic Model
"""

import pytest
from src.datachain.query.models import (
    BIQuery,
    BIDimension,
    BIFilter,
    BIMeasure,

    Aggregation,
    Comparator,
    DataType,

    SemanticModel,
    Table,
    SemanticColumn,
    KPI,
    SemanticMetric,
    SemanticBinaryMetric,
    SemanticComparison,
    Filter,
    Relationship,
)

from src.datachain.query.validators import validate_biquery_agaisnt_semantic_model

semantic_model = SemanticModel(
    tables=[
        Table(
            name="sales",
            description="Each row represents an individual order for a given customer and a given product",
            columns=[
                SemanticColumn(
                    name="order_id",
                    type=DataType.STRING,
                    description="The unique identifer for the order"
                ),
                SemanticColumn(
                    name="customer_id",
                    type=DataType.STRING,
                    description="The customers unique id"
                ),
                SemanticColumn(
                    name="product",
                    type=DataType.STRING,
                    description="The product code representing the product purchases"
                ),
                SemanticColumn(
                    name="price",
                    type=DataType.NUMERIC,
                    description="The unit price of the product"
                ),
                SemanticColumn(
                    name="quantity",
                    type=DataType.NUMERIC,
                    description="The quantity of the product purchased"
                ),
                SemanticColumn(
                    name="revenue",
                    type=DataType.NUMERIC,
                    description="The total revenue for this order (price * quantity)"
                ),
                SemanticColumn(
                    name="status",
                    type=DataType.STRING,
                    description="The order status. Can be 'Cancelled', 'Processing', 'On Hold', 'Delivered'"
                ),
                SemanticColumn(
                    name="order_date",
                    type=DataType.DATE,
                    description="The date the order was received"
                ),
                SemanticColumn(
                    name="delivery_date",
                    type=DataType.DATE,
                    description="The date the delivery was confirmed. Null if not delivered yet"
                ),
            ],
        ),
        Table(
            name="products",
            description="Each row represents a product sold by the company",
            columns=[
                SemanticColumn(
                    name="product_code",
                    type=DataType.STRING,
                    description="The unique product code"
                ),
                SemanticColumn(
                    name="product_name",
                    type=DataType.STRING,
                    description="The name of the product"
                ),
                SemanticColumn(
                    name="category",
                    type=DataType.STRING,
                    description="The category the product belongs to"
                ),
                SemanticColumn(
                    name="import_duty",
                    type=DataType.NUMERIC,  
                    description="The import duty for the product as a percentage"
                )
            ],
        ),
        Table(
            name="customers",
            description="Each row represents a customer",
            columns=[
                SemanticColumn(
                    name="customer_id",
                    type=DataType.STRING,
                    description="The unique identifer for the customer"
                ),
                SemanticColumn(
                    name="customer_name",
                    type=DataType.STRING,
                    description="The name of the customer"
                ),
                SemanticColumn(
                    name="region",
                    type=DataType.STRING,
                    description="The region the customer is located in"
                )
            ],
        )
    ],
    relationships=[
        Relationship(
            incoming="products",
            keys_incoming=[]
        )
    ],
    kpis=[
        KPI(
            name="kpi_total_revenue",
            expression=SemanticMetric(table="sales", column="revenue", aggregation=Aggregation.SUM),
            description="The sum of revenue across a set of orders",
            return_type=DataType.NUMERIC
        ),
        KPI(
            name="kpi_order_count",
            expression=SemanticMetric(table="sales", column="order_id", aggregation=Aggregation.COUNT),
            description="The count of orders",
            return_type=DataType.NUMERIC
        )
    ],
    filters=[
        Filter(
            name="filter_high_ticket_items",
            description="Predefined threshold for what the company defines as very valuable order",
            predicate=SemanticComparison(table="sales", column="price", comparator=Comparator.GREATER_THAN, value=10_000) 
        )
    ]
)


def test_simple_valid():
    query = BIQuery(
        dimensions=[BIDimension(table="sales", column="customer_id")],
        kpi_refs=["kpi_total_revenue"]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert is_valid

def test_valid_with_filter_ref():
    query = BIQuery(
        dimensions=[BIDimension(table="sales", column="customer_id")],
        kpi_refs=["kpi_total_revenue"],
        filter_refs=["filter_high_ticket_items"]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert is_valid

def test_valid_with_inline_filter():
    query = BIQuery(
        dimensions=[BIDimension(table="sales", column="customer_id")],
        kpi_refs=["kpi_total_revenue"],
        dimension_filters=[BIFilter(field="sales.quantity", comparator=Comparator.GREATER_THAN, value=1000)]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert is_valid

def test_valid_with_time_grain():
    query = BIQuery(
        dimensions=[BIDimension(table="sales", column="order_date", time_grain="MONTH")],
        kpi_refs=["kpi_total_revenue"]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert is_valid

def test_invalid_dimension_with_time_grain_not_date_type():
    query = BIQuery(
        dimensions=[BIDimension(table="sales", column="customer_id", time_grain="MONTH")],
        kpi_refs=["kpi_total_revenue"]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert not is_valid

def test_invalid_filter_ref_doesnt_exits():
    query = BIQuery(
        dimensions=[BIDimension(table="sales", column="customer_id")],
        kpi_refs=["kpi_total_revenue"],
        filter_refs=["filter_high_tick_items"]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert not is_valid

def test_invalid_with_inline_filter_field_doesnt_exist():
    query = BIQuery(
        dimensions=[BIDimension(table="sales", column="customer_id")],
        kpi_refs=["kpi_total_revenue"],
        dimension_filters=[BIFilter(field="sales.tax_rate", comparator=Comparator.GREATER_THAN, value=0.5)]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert not is_valid

def test_invalid_kpi_doesnt_not_exist():
    customer_sales_invalid = BIQuery(
        dimensions=[BIDimension(table="sales", column="customer_id")],
        kpi_refs=["kpi_total_sales"]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(customer_sales_invalid, semantic_model)

    print(errors)
    
    assert not is_valid

def test_invalid_dimension_doesnt_not_exist():
    customer_sales_invalid = BIQuery(
        dimensions=[BIDimension(table="sales", column="plant")],
        kpi_refs=["kpi_total_sales"]
    )

    is_valid, errors = validate_biquery_agaisnt_semantic_model(customer_sales_invalid, semantic_model)

    print(errors)
    
    assert not is_valid

def test_invalid_no_common_table():
    """

    """
    ...
    query = None

    is_valid, errors = validate_biquery_agaisnt_semantic_model(query, semantic_model)

    print(errors)

    assert not is_valid

if __name__ == "__main__":
    print(BIQuery.model_json_schema())
    print('\n\n\n')
    print(semantic_model.model_dump_json())