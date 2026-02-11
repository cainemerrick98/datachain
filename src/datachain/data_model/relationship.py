from dataclasses import dataclass
import ibis.expr.types as ir
import ibis
from typing import Callable
from .table import TableModel

@dataclass(frozen=True)
class Relationship:
    left: TableModel
    right: TableModel
    on: Callable[[ir.Table, ir.Table], ir.BooleanValue]
    how: str = "left"
