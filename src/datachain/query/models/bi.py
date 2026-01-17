"""
Defines the structure of a BIQuery - this is the format the LLM must match to and is compiled into SQL
"""

from pydantic import BaseModel, Field, model_validator, ValidationError
from pydantic_core import ErrorDetails
from .enums import Comparator, Aggregation, Sorting
from enum import Enum
from typing import Optional, Union, Literal


class TimeGrain(str, Enum):
    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


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
    name: str = Field(
        ...,
        description=(
            "Unique identifier for this measure. "
            "Used for filters and ordering. "
            "Example: 'total_revenue'"
        )
    )
    table: str = Field(
        ...,
        description="Name of the table the measure comes from. Example: 'orders'"
    )
    column: str = Field(
        ...,
        description="Column to aggregate. Example: 'revenue'"
    )
    aggregation: Aggregation = Field(
        ...,
        description=(
            "Aggregation function to apply to the column. "
            "Example: SUM, AVG, COUNT"
        )
    )
    window: Optional[Union[ChangeWindow, MovingAverageWindow]] = Field(
        None,
        description=(
            "Optional window function to apply to the measure. "
        )
    )

    @property
    def ref(self):
        return f"{self.table}.{self.column}"


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

    @model_validator(mode="after")
    def validate_query(self):

        errors: list[ErrorDetails] = []

        # Must select something
        if not any([self.dimensions, self.measures, self.kpi_refs]):
            errors.append({
                "type": "value_error",
                "loc": ("query",),
                "msg": "Query must select at least one dimension, measure, or KPI",
                "input": None,
                "ctx": {"error": "empty_query"},
            })


        dimension_names = {d.ref for d in self.dimensions}
        measure_names = {m.name for m in self.measures}
        valid_fields = dimension_names | measure_names | set(self.kpi_refs)

        # Order by must reference a valid field
        for idx, order_by in enumerate(self.order_by):
            if order_by.field not in valid_fields:
                errors.append({
                    "type": "value_error",
                    "loc": ("order_by", idx, "field"),
                    "msg": (
                        "Order by field must reference a dimension, "
                        "measure, or KPI defined in the query"
                    ),
                    "input": order_by.field,
                    "ctx": {"error": "invalid_field_reference"},
                })

        # Only one time grain dimension allowed
        time_grain_dimensions = [d for d in self.dimensions if d.time_grain is not None]
        if len(time_grain_dimensions) > 1:  
            errors.append({
                "type": "value_error",
                "loc": ("dimensions",),
                "msg": "Only one dimension with time grain is allowed per query",
                "input": [d.ref for d in time_grain_dimensions],
                "ctx": {"error": "multiple_time_grain_dimensions"},
            })

        # If a window function is applied there must be a time grain measure
        if any(m.window is not None for m in self.measures):
            has_time_grain_measure = any(
                d.time_grain is not None for d in self.dimensions
            )
            if not has_time_grain_measure:
                errors.append({
                    "type": "value_error",
                    "loc": ("measures",),
                    "msg": "If a window function is applied, there must be a time grain measure",
                    "input": [m.name for m in self.measures],
                    "ctx": {"error": "missing_time_grain_measure"},
                })

        if errors:
            raise ValidationError.from_exception_data(
                self.__class__.__name__,
                errors,
            )

        return self
