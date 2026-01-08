from pydantic import BaseModel
from enum import Enum
from typing import Literal


class Aggregation(Enum):
    """Aggregation functions for metric calculations"""
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    MIN = "MIN"
    MAX = "MAX"
    MEDIAN = "MEDIAN"


class Measure(BaseModel):
    column: str
    aggregation: Aggregation


class BIQuery(BaseModel):
    table: str
    dimensions: list[str]
    measures: list[Measure]
    kpi_refs: list[str]
    inline_filters: list[str]
    filter_refs: list[str]
    order_by: tuple[str, Literal['ASC', 'DESC']]



