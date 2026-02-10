from dataclasses import dataclass
from typing import TypeVar, Callable, Generic
from model_context import ModelContext
import ibis.expr.types as ir

ExprT = TypeVar("ExprT", bound=ir.Value)

@dataclass(frozen=True)
class Dimension(Generic[ExprT]):
    name: str
    expression: Callable[[ModelContext], ExprT]

@dataclass(frozen=True)
class Metric(Generic[ExprT]):
    name: str
    expression: Callable[[ModelContext], ExprT]

class MetricContext:
    def __init__(self, values: dict[str, ir.Value]):
        self._values = values

    def metric(self, metric: Metric) -> ir.Value:
        return self._values[metric.name]

@dataclass(frozen=True)
class DerivedMetric(Generic[ExprT]):
    name: str
    dependencies: list[Metric]
    expression: Callable[[MetricContext], ExprT]

@dataclass(frozen=True)
class Filter():
    name: str
    expression: Callable[[ModelContext], ir.BooleanValue]

METRIC_REGISTRY: dict[str, Metric] = {}
DIMENSION_REGISTRY: dict[str, Dimension] = {}
DERIVED_METRIC_REGISTRY: dict[str, DerivedMetric] = {}
FILTER_REGISTRY: dict[str, Filter] = {}

    
def metric(name: str):
    """Decorator to create a metric from a function."""
    def decorator(func: Callable[[ModelContext], ir.Value]) -> Metric:
        metric = Metric(name=name, expression=func)
        METRIC_REGISTRY[name] = metric
        return metric
    return decorator

def dimension(name: str):
    """Decorator to create a dimension from a function."""
    def decorator(func: Callable[[ModelContext], ir.Expr]) -> Dimension:
        dimension = Dimension(name=name, expression=func)
        DIMENSION_REGISTRY[name] = dimension
        return dimension
    return decorator

def derived_metric(name: str, dependencies: list[Metric]):
    """Decorator to create a derived metric from a function."""
    def decorator(func: Callable[[MetricContext], ir.Value]) -> DerivedMetric:
        derived_metric = DerivedMetric(name=name, dependencies=dependencies, expression=func)
        DERIVED_METRIC_REGISTRY[name] = derived_metric
        return derived_metric
    return decorator

def filter(name: str):
    """Decorator to create a filter from a function."""
    def decorator(func: Callable[[ModelContext], ir.BooleanValue]) -> Filter:
        filter = Filter(name=name, expression=func)
        FILTER_REGISTRY[name] = filter
        return filter
    return decorator