from dataclasses import dataclass, field
from typing import Literal

SortingDir = Literal["asc", "desc"]

@dataclass(frozen=True)
class BIQuery:
    """This class is exposed to query agent."""
    dimensions: list[str] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    metric_filters: list[str] = field(default_factory=list)
    orderby: list[tuple[str, SortingDir]] = field(default_factory=list)  # List of (column, direction) pairs
    limit: int | None = None
    offset: int | None = None
    distinct: bool = False