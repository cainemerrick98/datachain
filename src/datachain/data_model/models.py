from dataclasses import dataclass, field
import ibis.expr.types as ir
import ibis
from typing import Literal, Callable

ColumnType = Literal["int64", "float64", "string", "boolean", "timestamp"]

@dataclass
class TableModel:
    name: str
    schema: dict[str, ColumnType]
    _ibis_table: ir.Table = field(init=False, repr=False, default=None)

    def ibis(self) -> ir.Table:
        if self._ibis_table is None:
            self._ibis_table = ibis.table(self.schema, name=self.name)
        return self._ibis_table

    def __getitem__(self, key: str) -> ir.Column:
        return self.ibis()[key]
    

@dataclass(frozen=True)
class Relationship:
    left: TableModel
    right: TableModel
    on: Callable[[TableModel, TableModel], ir.BooleanValue]
    how: str = "left"
