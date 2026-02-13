from src.datachain.resolver import ResolutionResult, ResolvedQuery, resolve_query
from src.datachain.biquery import BIQuery
from src.datachain.data_model import ModelBuilder, ColumnType

builder = ModelBuilder()
@builder.table(name="users")
def users() -> dict[str, ColumnType]:
    return {
        "id": "int64",
        "name": "string",
    }

@builder.table(name="orders")
def orders() -> dict[str, ColumnType]:
    return {
        "id": "int64",
        "user_id": "int64",
        "amount": "float64",
    }

@builder.relationship(left=users, right=orders, how="left")
def user_orders_relationship(left, right):
    return left["id"] == right["user_id"] 

@builder.metric(name="total_order_amount", grain="orders")
def total_order_amount_metric(dm, sm):
    return dm["orders"]["amount"].sum()

@builder.dimension(name="user_name")
def user_name_dimension(dm):
    return dm["users"]["name"]

@builder.filter(name="high_value_orders")
def high_value_orders_filter(dm, sm):
    return dm["orders"]["amount"] > 100.0

def test_resolver():
    biquery = BIQuery(
        metrics=["total_order_amount"],
        dimensions=["user_name"],
        filters=["high_value_orders"],
    )
    result: ResolutionResult = resolve_query(biquery, builder._semantic_model, builder._data_model)
    assert result.success
    assert len(result.errors) == 0

    resolved_query = result.resolved_query
    assert resolved_query is not None
    assert resolved_query.metrics[0] is builder._semantic_model._metrics["total_order_amount"]
    assert resolved_query.dimensions[0] is builder._semantic_model._dimensions["user_name"]
    assert resolved_query.filters[0] is builder._semantic_model._filters["high_value_orders"]