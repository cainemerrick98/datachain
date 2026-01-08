from pydantic import BaseModel, Field, model_validator, ValidationError
from pydantic_core import ErrorDetails
from enums import Comparator, Aggregation


class BIDimension(BaseModel):
    table: str = Field(
        ...,
        description="Name of the table the dimension comes from. Example: 'orders'"
    )
    column: str = Field(
        ...,
        description="Column name to group by. Example: 'country'"
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


class BIFilter(BaseModel):
    field: str = Field(
        ...,
        description=(
            "Field to filter on. "
            "Must reference either a dimension column, "
            "a measure name, or a KPI reference."
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
    inline_filters: list[BIFilter] = Field(
        default_factory=list,
        description=(
            "List of filters applied to the query. "
            "Filters can reference dimensions, measures, or KPIs."
        )
    )
    filter_refs: list[str] = Field(
        default_factory=list,
        description=(
            "References to pre-defined filters in the semantic model."
        )
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
            })

        dimension_names = {d.column for d in self.dimensions}
        measure_names = {m.name for m in self.measures}
        valid_fields = dimension_names | measure_names | set(self.kpi_refs)

        for idx, f in enumerate(self.inline_filters):

            if f.field not in valid_fields:
                errors.append({
                    "type": "value_error",
                    "loc": ("inline_filters", idx, "field"),
                    "msg": (
                        "Filter field must reference a dimension, "
                        "measure, or KPI defined in the query"
                    ),
                    "input": f.field,
                })

            if f.value is None:
                errors.append({
                    "type": "value_error",
                    "loc": ("inline_filters", idx, "value"),
                    "msg": "Filter must include a value",
                    "input": f.value,
                })

        if errors:
            raise ValidationError.from_exception_data(
                self.__class__.__name__,
                errors,
            )

        return self
