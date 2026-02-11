from dataclasses import dataclass
from typing import Callable
from model_context import ModelContext
import ibis.expr.types as ir
from .models import Metric, Dimension, DerivedMetric, Filter, MetricContext
from .registry import METRIC_REGISTRY, DIMENSION_REGISTRY, DERIVED_METRIC_REGISTRY, FILTER_REGISTRY

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