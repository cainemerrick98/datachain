from dataclasses import dataclass, field
import ibis.expr.types as ir
import ibis
from typing import Literal, Callable
import ibis.expr.types as ir

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

class DataModel():
    def __init__(
        self,
    ):
        self._tables: dict[str, TableModel] = {}
        self._relationships: list[Relationship] = []

    def get_table(self, name: str) -> TableModel | None:
        return self._tables.get(name)
    
    def register_table(self, table: TableModel):
        setattr(self, table.name, table.ibis())
        self._tables[table.name] = table

    def register_relationship(self, relationship: Relationship):
        self._relationships.append(relationship)

    def get_relationship_graph(self, directed: bool = True) -> dict[str, list[Relationship]]:
        graph = {table.name: [] for table in self._tables.values()}
        for rel in self._relationships:
            graph[rel.left.name].append(rel)
            if not directed:
                graph[rel.right.name].append(rel)
        return graph