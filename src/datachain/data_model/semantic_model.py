from dataclasses import dataclass, field
import ibis.expr.types as ir
from typing import TypeVar, Callable, Generic
from .data_model import DataModel
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
    expression: Callable[[DataModel, "SemanticModel"], ExprT]
    _cached_expr: ExprT | None = None

    def resolve(self, data_model: DataModel, semantic_model: "SemanticModel") -> ExprT:
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
    expression: Callable[[DataModel, "SemanticModel"], ir.BooleanValue]
    _cached_expr: ir.BooleanValue | None = None

    def resolve(self, data_model: DataModel, semantic_model: "SemanticModel") -> ir.BooleanValue:
        if self._cached_expr is not None:
            return self._cached_expr
        expr = self.expression(data_model, semantic_model)
        self._cached_expr = expr
        return expr


class SemanticModel():
    def __init__(self):
        self._metrics: dict[str, Metric] = {}
        self._dimensions: dict[str, Dimension] = {}
        self._filters: dict[str, Filter] = {}

    def register_metric(self, metric: Metric):
        self._metrics[metric.name] = metric

    def register_dimension(self, dimension: Dimension):
        self._dimensions[dimension.name] = dimension

    def register_filter(self, filter: Filter):
        self._filters[filter.name] = filter

    def get_metric(self, name: str) -> Metric | None:
        return self._metrics.get(name)
    
    def get_dimension(self, name: str) -> Dimension | None: 
        return self._dimensions.get(name)
    
    def get_filter(self, name: str) -> Filter | None:
        return self._filters.get(name)
    
