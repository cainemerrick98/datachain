from .models import TableModel, Relationship, ColumnType
from typing import Callable
from .data_model import data_model
import ibis.expr.types as ir

def table(name: str) -> TableModel:
    """Decorator to create a table model from a function."""
    def decorator(func: Callable[[], dict[str, ColumnType]]) -> TableModel:
        schema = func()
        table_model = TableModel(name=name, schema=schema)
        data_model.register_table(table_model)
        return table_model
    return decorator


def relationship(left: TableModel, right: TableModel, how: str = "left"):
    """Decorator to create a relationship between two table models."""
    def decorator(
        func: Callable[[ir.Table, ir.Table], ir.BooleanValue]
    ) -> Relationship:
        rel = Relationship(left=left, right=right, on=func, how=how)
        data_model.register_relationship(rel)
        return rel
    return decorator