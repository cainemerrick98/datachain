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
