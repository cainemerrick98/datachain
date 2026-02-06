"""
Contains a complete, complex and valid semantic model to be used across the testing suite
"""
from src.datachain.query.models import (
    SemanticModel,
    Table,
    SemanticColumn,
    DataType,
    Relationship,
    RelationshipType,
    KPI,
    Filter,
    SemanticComparison,
    SemanticKPIComparison,
    SemanticBinaryMetric,
    SemanticMetric,
    Aggregation,
    Arithmetic,
    Comparator
)

# Dimension tables
customer = Table(
    name="Customer",
    description="Customer dimension",
    columns=[
        SemanticColumn("customer_id", DataType.STRING, "Customer primary key"),
        SemanticColumn("country", DataType.STRING, "Customer country"),
        SemanticColumn("is_active", DataType.BOOLEAN, "Whether the customer is active"),
    ],
)

product = Table(
    name="Product",
    description="Product dimension",
    columns=[
        SemanticColumn("product_id", DataType.STRING, "Product primary key"),
        SemanticColumn("category", DataType.STRING, "Product category"),
    ],
)

date = Table(
    name="Date",
    description="Date dimension",
    columns=[
        SemanticColumn("date_id", DataType.DATE, "Calendar date"),
        SemanticColumn("year", DataType.NUMERIC, "Year"),
        SemanticColumn("month", DataType.NUMERIC, "Month"),
    ],
)

# Fact table
sales = Table(
    name="Sales",
    description="Sales fact table",
    columns=[
        SemanticColumn("sale_id", DataType.STRING, "Sale primary key"),
        SemanticColumn("customer_id", DataType.STRING, "FK to Customer"),
        SemanticColumn("product_id", DataType.STRING, "FK to Product"),
        SemanticColumn("date_id", DataType.DATE, "FK to Date"),
        SemanticColumn("revenue", DataType.NUMERIC, "Revenue amount"),
        SemanticColumn("quantity", DataType.NUMERIC, "Units sold"),
    ],
)

# Relationships
relationships = [
    Relationship(
        incoming="Customer",
        keys_incoming=["customer_id"],
        type=RelationshipType.ONE_TO_MANY,
        outgoing="Sales",
        keys_outgoing=["customer_id"],
    ),
    Relationship(
        incoming="Product",
        keys_incoming=["product_id"],
        type=RelationshipType.ONE_TO_MANY,
        outgoing="Sales",
        keys_outgoing=["product_id"],
    ),
    Relationship(
        incoming="Date",
        keys_incoming=["date_id"],
        type=RelationshipType.ONE_TO_MANY,
        outgoing="Sales",
        keys_outgoing=["date_id"],
    ),
]

# Base KPIs
total_revenue = KPI(
    name="total_revenue",
    description="Total revenue",
    return_type=DataType.NUMERIC,
    expression=SemanticMetric(
        table="Sales",
        column="revenue",
        aggregation=Aggregation.SUM,
    ),
)

total_quantity = KPI(
    name="total_quantity",
    description="Total units sold",
    return_type=DataType.NUMERIC,
    expression=SemanticMetric(
        table="Sales",
        column="quantity",
        aggregation=Aggregation.SUM,
    ),
)

# Derived KPIs
average_price = KPI(
    name="average_price",
    description="Average revenue per unit sold",
    return_type=DataType.NUMERIC,
    expression=SemanticBinaryMetric(
        left="total_revenue",
        operator=Arithmetic.DIV,
        right="total_quantity",
    ),
)


# Column Filters
active_customers = Filter(
    name="active_customers",
    description="Only active customers",
    predicate=SemanticComparison(
        table="Customer",
        column="is_active",
        comparator=Comparator.EQUAL,
        value=True,
    ),
)

# KPI filter
high_revenue = Filter(
    name="high_revenue",
    description="Only include results with revenue above 10k",
    predicate=SemanticKPIComparison(
        name="high_revenue_check",
        kpi=total_revenue,
        comparator=Comparator.GREATER_THAN,
        value=10_000,
    ),
)

# Final Semantic Model
semantic_model = SemanticModel(
    tables=[sales, customer, product, date],
    relationships=relationships,
    kpis=[total_revenue, total_quantity, average_price],
    filters=[active_customers, high_revenue],
)

