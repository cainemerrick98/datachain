from .table import TableModel, ColumnType
from .relationship import Relationship
from typing import Callable
from .registry import TABLE_REGISTRY, RELATIONSHIP_REGISTRY
import ibis.expr.types as ir



def table(name: str) -> TableModel:
    """Decorator to create a table model from a function."""
    def decorator(func: Callable[[], dict[str, ColumnType]]) -> TableModel:
        schema = func()
        table_model = TableModel(name=name, schema=schema)
        TABLE_REGISTRY[name] = table_model
        return table_model
    return decorator


def relationship(left: TableModel, right: TableModel, how: str = "left"):
    """Decorator to create a relationship between two table models."""
    def decorator(
        func: Callable[[ir.Table, ir.Table], ir.BooleanValue]
    ) -> Relationship:
        rel = Relationship(left=left, right=right, on=func, how=how)
        RELATIONSHIP_REGISTRY.append(rel)
        return rel
    return decorator