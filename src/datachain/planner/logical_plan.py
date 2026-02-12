from dataclasses import dataclass
from datachain.data_model import TableModel, Relationship
from ..errors import DataChainError

@dataclass()
class LogicalPlan:
    base_table: TableModel
    joins: list[Relationship]


@dataclass()
class PlanningResult:
    success: bool
    logical_plan: LogicalPlan
    errors: list[DataChainError]
