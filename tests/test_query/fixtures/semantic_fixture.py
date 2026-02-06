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

# =====================
# Dimension tables
# =====================

customer = Table(
    name="Customer",
    description="Customer dimension",
    columns=[
        SemanticColumn(
            name="customer_id",
            type=DataType.STRING,
            description="Customer primary key",
        ),
        SemanticColumn(
            name="country",
            type=DataType.STRING,
            description="Customer country",
        ),
        SemanticColumn(
            name="is_active",
            type=DataType.BOOLEAN,
            description="Whether the customer is active",
        ),
    ],
)

product = Table(
    name="Product",
    description="Product dimension",
    columns=[
        SemanticColumn(
            name="product_id",
            type=DataType.STRING,
            description="Product primary key",
        ),
        SemanticColumn(
            name="category",
            type=DataType.STRING,
            description="Product category",
        ),
    ],
)

date = Table(
    name="Date",
    description="Date dimension",
    columns=[
        SemanticColumn(
            name="date_id",
            type=DataType.DATE,
            description="Calendar date",
        ),
        SemanticColumn(
            name="year",
            type=DataType.NUMERIC,
            description="Year",
        ),
        SemanticColumn(
            name="month",
            type=DataType.NUMERIC,
            description="Month",
        ),
    ],
)

# =====================
# Fact table
# =====================

sales = Table(
    name="Sales",
    description="Sales fact table",
    columns=[
        SemanticColumn(
            name="sale_id",
            type=DataType.STRING,
            description="Sale primary key",
        ),
        SemanticColumn(
            name="customer_id",
            type=DataType.STRING,
            description="FK to Customer",
        ),
        SemanticColumn(
            name="product_id",
            type=DataType.STRING,
            description="FK to Product",
        ),
        SemanticColumn(
            name="date_id",
            type=DataType.DATE,
            description="FK to Date",
        ),
        SemanticColumn(
            name="revenue",
            type=DataType.NUMERIC,
            description="Revenue amount",
        ),
        SemanticColumn(
            name="quantity",
            type=DataType.NUMERIC,
            description="Units sold",
        ),
    ],
)

# =====================
# Relationships
# =====================

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

# =====================
# Base KPIs
# =====================

total_revenue = KPI(
    name="kpi_total_revenue",
    description="Total revenue",
    return_type=DataType.NUMERIC,
    expression=SemanticMetric(
        table="Sales",
        column="revenue",
        aggregation=Aggregation.SUM,
    ),
)

total_quantity = KPI(
    name="kpi_total_quantity",
    description="Total units sold",
    return_type=DataType.NUMERIC,
    expression=SemanticMetric(
        table="Sales",
        column="quantity",
        aggregation=Aggregation.SUM,
    ),
)

# =====================
# Derived KPI
# =====================

average_price = KPI(
    name="kpi_average_price",
    description="Average revenue per unit sold",
    return_type=DataType.NUMERIC,
    expression=SemanticBinaryMetric(
        left="kpi_total_revenue",
        operator=Arithmetic.DIV,
        right="kpi_total_quantity",
    ),
)

# =====================
# Column Filter
# =====================

active_customers = Filter(
    name="filter_active_customers",
    description="Only active customers",
    predicate=SemanticComparison(
        table="Customer",
        column="is_active",
        comparator=Comparator.EQUAL,
        value=True,
    ),
)

# =====================
# KPI Filter
# =====================

high_revenue = Filter(
    name="filter_high_revenue",
    description="Only include results with revenue above 10k",
    predicate=SemanticKPIComparison(
        name="high_revenue_check",
        kpi=total_revenue,
        comparator=Comparator.GREATER_THAN,
        value=10_000,
    ),
)

# =====================
# Final Semantic Model
# =====================

semantic_model = SemanticModel(
    tables=[sales, customer, product, date],
    relationships=relationships,
    kpis=[total_revenue, total_quantity, average_price],
    filters=[active_customers, high_revenue],
)
