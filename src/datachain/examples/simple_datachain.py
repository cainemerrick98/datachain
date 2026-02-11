from ..data_model import table, relationship
from ..semantic import metric, dimension, filter

# Data Model

@table(name="users")
def users():
    return {
        "id": "int64",
        "name": "string",
        "email": "string",
        }

@table(name="orders")
def orders():
    return {
        "id": "int64",
        "user_id": "int64",
        "amount": "float64",
        "order_date": "timestamp",
        }

@relationship(
    name="user_orders",
    from_table=users,
    to_table=orders,
)
def user_orders(l, r):
    return l["id"] == r["user_id"]


# Semantic Model
@metric(name="total_revenue")
def total_revenue(dm):
    return dm.orders["amount"].sum()


if __name__ == "__main__":
    print(total_revenue.expression)