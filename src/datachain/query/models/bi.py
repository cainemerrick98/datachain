"""
Defines the structure of a BIQuery - this is the format the LLM must match to and is compiled into SQL
"""

from pydantic import BaseModel, Field, model_validator, ValidationError
from pydantic_core import ErrorDetails
from .enums import Comparator, Aggregation, Sorting, TimeGrain
from typing import Optional, Union, Literal

class BIDimension(BaseModel):
    table: str = Field(
        ...,
        description="Name of the table the dimension comes from. Example: 'orders'"
    )
    column: str = Field(
        ...,
        description="Column name to group by. Example: 'country'"
    )
    time_grain: Optional[TimeGrain] = Field(
        None,
        description=(
            "Optional time grain for date dimensions. "
            "Example: DAY, MONTH, YEAR"
        )
    )

    @property
    def ref(self):
        return f"{self.table}.{self.column}"


class ChangeWindow(BaseModel):
    period: int = Field(
        ...,    
        description="The number of units for the change window. Example: 7"
    )
    mode: Literal["ABSOLUTE", "PERCENTAGE"] = Field(
        ...,
        description="The mode of change calculation. Either ABSOLUTE or PERCENTAGE"
    )


class MovingAverageWindow(BaseModel):
    period: int = Field(
        ...,
        description="The number of units for the moving average window. Example: 7"
    )
    mode: Literal["AHEAD", "BEHIND", "CENTERED"] = Field(
        ...,
        description=(
            "The mode of moving average calculation. Either AHEAD, BEHIND, or CENTERED",
            "AHEAD looks forward, BEHIND looks backward, CENTERED looks both ways"
            )
    )


class BIMeasure(BaseModel):
    model_config = {"frozen": True}

    name: str
    table: str
    column: str
    aggregation: Aggregation
    window: Optional[Union[ChangeWindow, MovingAverageWindow]] = None

    def _identity_key(self) -> tuple:
        # Identity = base aggregation only
        return (self.table, self.column, self.aggregation)

    def __eq__(self, other):
        if not isinstance(other, BIMeasure):
            return NotImplemented
        return self._identity_key() == other._identity_key()

    def __hash__(self):
        return hash(self._identity_key())


class BIFilter(BaseModel):
    field: str = Field(
        ...,
        description=(
            "Field to filter on. Must be one of:\n"
            "1) Dimension in 'table.column' format, e.g. 'orders.country'\n"
            "2) Measure name, e.g. 'total_revenue'\n"
            "3) KPI name e.g. 'kpi_gross_margin'"
        )
    )
    comparator: Comparator = Field(
        ...,
        description=(
            "Comparison operator. "
            "Examples: '=', '!=', '>', '<', 'IN'"
        )
    )
    value: str | int | float | list[str] = Field(
        ...,
        description=(
            "Value to compare against. "
            "Examples: 'US', 1000, ['US','CA']"
        )
    )

    @property
    def table(self) -> Optional[str]:
        """Extracts the table name if the field is a dimension in table.column format"""
        if '.' in self.field:
            return self.field.split('.')[0]
        return None
    
    @property
    def column(self) -> Optional[str]:
        """Extracts the column name if the field is a dimension in table.column format"""
        if '.' in self.field:
            return self.field.split('.')[1]
        return None

 
class BIOrderBy(BaseModel):
    """Specifies sorting for query results"""
    field: str = Field(
        description=(
            "Field to sort by. Can be a dimension, a measure, or a kpi.",
            "The referenced column must be in the query"
        )
    )
    sorting: Sorting = Field(
        default=Sorting.ASC,
        description="Sort direction: ASC for ascending (lowest to highest), DESC for descending (highest to lowest). Defaults to ASC."
    )

#TODO Handle distinct
class BIQuery(BaseModel):
    dimensions: list[BIDimension] = Field(
        default_factory=list,
        description=(
            "List of dimensions to group by. "
            "Leave empty if only selecting measures."
        )
    )
    measures: list[BIMeasure] = Field(
        default_factory=list,
        description=(
            "List of measures to calculate. "
            "Each measure must have a unique name."
        )
    )
    kpi_refs: list[str] = Field(
        default_factory=list,
        description=(
            "List of KPI names defined in the semantic model. "
            "Example: ['gross_margin']"
        )
    )
    measure_filters: list[BIFilter] = Field(
        default_factory=list,
        description=(
            "List of the filters that take a measure name as the field"
        )
    )
    kpi_filters: list[BIFilter] = Field(
        default_factory=list,
        description=(
            "List of the filters that take a kpi name as the field"
        )
    )
    dimension_filters: list[BIFilter] = Field(
        default_factory=list,
        description=(
            "List of the filters that take a dimension as the field",
            "The field name must fomratted as table.column when using a dimension filter"
        )
    )
    filter_refs: list[str] = Field(
        default_factory=list,
        description=(
            "References to pre-defined filters in the semantic model."
        )
    )
    order_by: list[BIOrderBy] = Field(
        default_factory=list,
        description=(
            "List of order by clauses applied to the query",
            "Each clause requires a reference to a dimension, measure or KPI and a sorting direction"
        )
    )
    limit: Optional[int] = Field(
        None,
        description="The number of rows to restrict the query result to"
    )

# Resolved BI

class ResolvedBIDimension(BaseModel):
    table: str
    column: str


class ResolvedBIDimensionTimeGrain(BaseModel):
    time_grain: TimeGrain
    table: str
    column: str


class ResolvedBIFilter(BaseModel):
    """
    Includes the table and column name
    """
    table: str
    column: str
    comparator: Comparator
    value: str | int | float | list[str] 


class ResolvedBIMeasureFilter(BaseModel):
    """
    Directly includes the measure in the filter
    """
    measure: BIMeasure
    comparator: Comparator
    value: int | float


class ResolvedOrderByDimension(BaseModel):
    """Ensures the order by has a concrete reference"""
    table: str
    column: str
    sorting: Sorting


class ResolvedOrderByMeasure(BaseModel):
    """Ensures sorting by measure has a conrete reference"""
    measure: BIMeasure
    sorting: Sorting


class ResolvedBIQuery(BaseModel):
    """
    No longer holds any semantic references
    """
    dimensions: list[ResolvedBIDimension] = Field(
        default_factory=list,
    )
    time_grained_dimensions: list[ResolvedBIDimensionTimeGrain] = Field(
        default_factory=list
    )
    measures: list[BIMeasure] = Field(
        default_factory=list,
    )
    measure_filters: list[ResolvedBIMeasureFilter] = Field(
        default_factory=list,
    )
    dimension_filters: list[ResolvedBIFilter] = Field(
        default_factory=list,
    )
    order_by: list[BIOrderBy] = Field(
        default_factory=list,
    )
    limit: Optional[list[Union[ResolvedOrderByDimension, ResolvedOrderByMeasure]]] = Field(
        None,
    )
