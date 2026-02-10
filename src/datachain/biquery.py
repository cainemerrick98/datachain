from dataclasses import dataclass

@dataclass(frozen=True)
class BIQuery:
    """This class is exposed to query agent."""
    dimensions: list[str]
    metrics: list[str]
    filters: list[str]
    metric_filters: list[str]
    orderby: list[str]
    limit: int | None = None
    offset: int | None = None
    distinct: bool = False