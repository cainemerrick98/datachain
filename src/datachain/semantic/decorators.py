from dataclasses import dataclass
from typing import Callable
from data_model.data_model import DataModel
import ibis.expr.types as ir
from .models import Metric, Dimension, Filter
from .semantic_model import semantic_model

def metric(name: str):
    """Decorator to create a metric from a function."""
    def decorator(func: Callable[[DataModel], ir.Value]) -> Metric:
        metric = Metric(name=name, expression=func)
        semantic_model.register_metric(metric)
        return metric
    return decorator

def dimension(name: str):
    """Decorator to create a dimension from a function."""
    def decorator(func: Callable[[DataModel], ir.Expr]) -> Dimension:
        dimension = Dimension(name=name, expression=func)
        semantic_model.register_dimension(dimension)
        return dimension
    return decorator


def filter(name: str):
    """Decorator to create a filter from a function."""
    def decorator(func: Callable[[DataModel], ir.BooleanValue]) -> Filter:
        filter = Filter(name=name, expression=func)
        semantic_model.register_filter(filter)
        return filter
    return decorator