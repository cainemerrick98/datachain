from dataclasses import dataclass, field
from typing import Optional, Any
from .models import SQLQuery

@dataclass
class QueryContext:
    # Discovered during resolution
    tables: set[str] = field(default_factory=set)
    common_table: Optional[str] = None

    # Discovered during planning
    joins: list = field(default_factory=list)
    requires_cte: bool = False
    unique_measures: list = field(default_factory=list)
    window_measures: list = field(default_factory=list)
    window_measure_map: list = field(default_factory=dict)

    # Diagnostics
    warnings: list[str] = field(default_factory=list)
    trace: list[str] = field(default_factory=list)


@dataclass
class QueryError():
    stage: str
    msg: str
    details: Optional[Any] = None 


@dataclass
class QueryResult:
    sql_query: SQLQuery | None
    errors: list[QueryError] | None
    context: QueryContext
