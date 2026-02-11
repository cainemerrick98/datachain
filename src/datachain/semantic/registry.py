from .models import Metric, Dimension, DerivedMetric, Filter
from typing import Callable

METRIC_REGISTRY: dict[str, Metric] = {}
DIMENSION_REGISTRY: dict[str, Dimension] = {}
DERIVED_METRIC_REGISTRY: dict[str, DerivedMetric] = {}
FILTER_REGISTRY: dict[str, Filter] = {}