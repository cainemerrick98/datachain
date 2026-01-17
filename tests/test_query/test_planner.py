"""
Ensures that the BIQuery is correctly planned as a SQLQuery
"""
from src.datachain.query.models import (
    SemanticModel,
    Table,
    SemanticColumn,
    KPI,
    Filter,
    Relationship,
    RelationshipType,
    SemanticMetric,
    SemanticComparison,
    SemanticBinaryMetric,
    SemanticKPIComparison,

    Arithmetic,
    Aggregation,
    Comparator,
    DataType,

    BIQuery,
    BIDimension,
    BIFilter,
    BIMeasure,
    BIOrderBy,

    SQLQuery,
    SQLMeasure,
    BinaryMetric,
    Comparison,
    ColumnComparison,
    And,
    Or,
    Not
)
from src.datachain.query.planner import transform_bi_query_to_sql_query

orders = Table(
    name="orders",
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
            description="The unique identifier for the customer"
        ),
        SemanticColumn(
            name="product_code",
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
            name="order_date",
            type=DataType.DATE,
            description="The date the order was placed"
        ),
        SemanticColumn(
            name="production_cost",
            type=DataType.NUMERIC,
            description="The cost to produce the product"
        )
    ]
)

customers = Table(
    name="customers",
    description="Each row represents a customer",
    columns=[
        SemanticColumn(
            name="id",
            type=DataType.STRING,
            description="The unique identifer for the customer"
        ),
        SemanticColumn(
            name="customer",
            type=DataType.STRING,
            description="The customers name"
        ),
        SemanticColumn(
            name="customer_name",
            type=DataType.STRING,
            description="The full name of the customer"
        ),
        SemanticColumn(
            name="region",
            type=DataType.STRING,
            description="The region the customer is located in"
        ),
    ]
)

products = Table(
    name="products",
    description="Each row represents a product",
    columns=[
        SemanticColumn(
            name="product_code",
            type=DataType.STRING,
            description="The product code representing the product purchases"
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
    ]
)

semantic_model = SemanticModel(
    tables=[orders, customers, products],
    relationships=[
        Relationship(
            incoming="customers",
            keys_incoming=["id"],
            type=RelationshipType.ONE_TO_MANY,
            outgoing="orders",
            keys_outgoing=["customer_id"],
        ),
        Relationship(
            incoming="products",
            keys_incoming=["product_code"],
            type=RelationshipType.ONE_TO_MANY,
            outgoing="orders",
            keys_outgoing=["product_code"],
        ),
    ],
    kpis=[
        KPI(
            name="total_revenue",
            expression=SemanticMetric(
                table="orders",
                column="revenue",
                aggregation=Aggregation.SUM,
            ),
            description="Total revenue from all orders",
            return_type=DataType.NUMERIC,
        ),
        KPI(
            name="average_order_value",
            expression=SemanticMetric(
                table="orders",
                column="revenue",
                aggregation=Aggregation.AVG,
            ),
            description="Average revenue per order",
            return_type=DataType.NUMERIC,
        ),
        KPI(
            name="total_cost",
            expression=SemanticMetric(
                table="orders",
                column="production_cost",
                aggregation=Aggregation.SUM,
            ),
            description="Total production cost from all orders",
            return_type=DataType.NUMERIC,
        ),
        KPI(
            name="total_profit",
            expression=SemanticBinaryMetric(
                left="total_revenue",
                operator="-",
                right="total_cost",
            ),
            description="Total profit from all orders",
            return_type=DataType.NUMERIC,
        ),
        KPI(
            name="profit_margin",
            expression=SemanticBinaryMetric(
                left="total_profit",
                operator="/",
                right="total_revenue",
            ),
            description="Profit margin as a percentage of total revenue",
            return_type=DataType.NUMERIC,
        )
    ],
    filters=[
        Filter(
            name="high_value_customers",
            predicate=SemanticComparison(
                table="orders",
                column="revenue",
                comparator=Comparator.GREATER_THAN,
                value=1000,
            ),
            description="Customers with orders over $1000"
        ),
        Filter(
            name="target_regions",
            predicate=SemanticComparison(
                table="customers",
                column="region",
                comparator=Comparator.IN,
                value=["North", "East", "West"],
            ),
            description="Customers located in target regions"
        ),
    ],

)

def test_transform_bi_query_to_sql_query():
    bi_query = BIQuery(
        dimensions=[
            BIDimension(
                table="customers",
                column="customer_name",
            ),
        ],
        kpi_refs=[
            "total_revenue",
            "profit_margin",
        ],
        filter_refs=[
            "high_value_customers",
            "target_regions",
        ],
        order_by=[
            BIOrderBy(
                field="customers.customer_name",
                ascending=True,
            )
        ]
    )

    sql_query = transform_bi_query_to_sql_query(bi_query, semantic_model)

    assert sql_query is not None
    assert len(sql_query.columns) == 3  # 1 dimension + 2 kpis
    assert len(sql_query.joins) == 1  # Join between customers and orders
    assert len(sql_query.filters) == 2  # 2 filters
    assert len(sql_query.order_by) == 1  # 1 order by clause
    assert sql_query.table_name == "orders"  # From table is orders
    assert len(sql_query.group_by) == 1  # Group by customer_name
    assert len(sql_query.having) == 0  # No having clauses yet
    assert sql_query.limit is None  # No limit specified
    assert sql_query.offset is None  # No offset specified