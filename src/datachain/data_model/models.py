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
