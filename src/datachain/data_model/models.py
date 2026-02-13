from dataclasses import dataclass, field
import ibis.expr.types as ir
import ibis
from typing import Literal, Callable
from typing import TypeVar, Callable, Generic
from .data_model import DataModel
from .semantic_model import SemanticModel
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

ExprT = TypeVar("ExprT", bound=ir.Value)

@dataclass()
class Dimension(Generic[ExprT]):
    name: str
    expression: Callable[[DataModel], ExprT]
    _cached_expr: ExprT | None = None

    def resolve(self, data_model: DataModel) -> ExprT:
        if self._cached_expr is not None:
            return self._cached_expr
        expr = self.expression(data_model)
        self._cached_expr = expr
        return expr

@dataclass()
class Metric(Generic[ExprT]):
    """Can either be a raw metric or a derived metric."""
    name: str
    grain: str
    dependencies: list["Metric"]
    expression: Callable[[DataModel, SemanticModel], ExprT]
    _cached_expr: ExprT | None = None

    def resolve(self, data_model: DataModel, semantic_model: SemanticModel) -> ExprT:
        if self._cached_expr is not None:
            return self._cached_expr
        # resolve dependencies first
        for dep in self.dependencies:
            dep.resolve(data_model, semantic_model)
        expr = self.expression(data_model, semantic_model)
        self._cached_expr = expr
        return expr

@dataclass()
class Filter():
    name: str
    expression: Callable[[DataModel, SemanticModel], ir.BooleanValue]
    _cached_expr: ir.BooleanValue | None = None

    def resolve(self, data_model: DataModel, semantic_model: SemanticModel) -> ir.BooleanValue:
        if self._cached_expr is not None:
            return self._cached_expr
        expr = self.expression(data_model, semantic_model)
        self._cached_expr = expr
        return expr

