from dataclasses import dataclass, field
from ..biquery import SortingDir
from ..data_model import Metric, Dimension, Filter

@dataclass(frozen=True)
class ResolvedQuery:
    dimensions: list[Dimension] = field(default_factory=list)
    metrics: list[Metric] = field(default_factory=list)
    filters: list[Filter] = field(default_factory=list)
    metric_filters: list[Filter] = field(default_factory=list)
    orderby: list[tuple[Dimension | Metric, SortingDir]] = field(default_factory=list)  # List of (column, direction) pairs
    limit: int | None = None
    offset: int | None = None
    distinct: bool = False
