from dataclasses import dataclass
from typing import TypeVar, Callable, Generic
from ..data_model.data_model import DataModel
from .semantic_model import SemanticModel
import ibis.expr.types as ir

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

