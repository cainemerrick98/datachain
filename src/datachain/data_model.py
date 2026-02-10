from dataclasses import dataclass
import ibis.expr.types as ir
import ibis
from typing import Literal, Callable

ColumnType = Literal["int64", "float64", "string", "boolean", "timestamp"]

@dataclass(frozen=True)
class TableModel:
    name: str
    schema: dict[str, ColumnType]

    def ibis(self) -> ir.Table:
        """Create an Ibis table expression for this table model."""
        return ibis.table(self.name, self.schema)
    

@dataclass(frozen=True)
class Relationship:
    left: TableModel
    right: TableModel
    on: Callable[[ir.Table, ir.Table], ir.BooleanValue]
    how: str = "left"


TABLE_REGISTRY: dict[str, TableModel] = {}
RELATIONSHIP_REGISTRY: list[Relationship] = []


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