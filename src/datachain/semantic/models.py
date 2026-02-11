from dataclasses import dataclass
from typing import TypeVar, Callable, Generic
from ..data_model.data_model import DataModel
from .semantic_model import SemanticModel
import ibis.expr.types as ir

ExprT = TypeVar("ExprT", bound=ir.Value)

@dataclass(frozen=True)
class Dimension(Generic[ExprT]):
    name: str
    expression: Callable[[DataModel], ExprT]

@dataclass(frozen=True)
class Metric(Generic[ExprT]):
    """Can either be a raw metric or a derived metric."""
    name: str
    grain: str
    dependencies: list["Metric"]
    expression: Callable[[DataModel, SemanticModel], ExprT]

@dataclass(frozen=True)
class Filter():
    name: str
    expression: Callable[[DataModel, SemanticModel], ir.BooleanValue]

