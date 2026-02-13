from src.datachain.data_model import ModelBuilder, DataModel, SemanticModel, Relationship, TableModel, ColumnType


def test_model_builder():
    builder = ModelBuilder()
    data_model: DataModel = builder.data_model
    semantic_model: SemanticModel = builder._semantic_model

    @builder.table(name="users")
    def users() -> dict[str, ColumnType]:
        return {
            "id": "int",
            "name": "string",
        }

    @builder.table(name="orders")
    def orders() -> dict[str, ColumnType]:
        return {
            "id": "int",
            "user_id": "int",
            "amount": "float",
        }

    @builder.relationship(left=users, right=orders, how="left")
    def user_orders_relationship(left: TableModel, right: TableModel):
        return left["id"] == right["user_id"]

    @builder.metric(name="total_order_amount", grain="user")
    def total_order_amount_metric(dm: DataModel, sm: SemanticModel):
        return dm["orders"]["amount"].sum()

    @builder.dimension(name="user_name")
    def user_name_dimension(dm: DataModel):
        return dm["users"]["name"]

    @builder.filter(name="high_value_orders")
    def high_value_orders_filter(dm: DataModel):
        return dm["orders"]["amount"] > 100.0   

    # Tables exist
    assert data_model.get_table("users") is not None
    assert data_model.get_table("orders") is not None

    # Relationship exists
    rel = data_model._relationships[0]
    assert isinstance(rel, Relationship)
    # Relationship has correct types
    assert rel.left == data_model.get_table("users")
    assert rel.right == data_model.get_table("orders")
    assert rel.on() 

    assert len(data_model.get_relationship_graph()[data_model.get_table("users")]) == 1
    assert len(semantic_model._metrics) == 1
    assert len(semantic_model._dimensions) == 1
    assert len(semantic_model._filters) == 1