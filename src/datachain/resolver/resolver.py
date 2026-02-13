from ..biquery import BIQuery
from .resolved import ResolvedQuery
from ..data_model import DataModel, SemanticModel
from ..errors import DataChainError
from dataclasses import dataclass, field

@dataclass()
class ResolutionResult:
    success: bool
    resolved_query: ResolvedQuery
    errors: list[DataChainError] = field(default_factory=list)

def resolve_query(
    biquery: BIQuery,
    semantic_model: SemanticModel,
    data_model: DataModel
) -> ResolutionResult:

    errors: list[DataChainError] = []

    # Resolve dimensions
    dimensions = []
    for dim_name in biquery.dimensions:
        dim = semantic_model.get_dimension(dim_name)

        if dim is None:
            errors.append(DataChainError(
                stage="resolution",
                code="dimension_not_found",
                message=f"Dimension '{dim_name}' not found in semantic model."
            ))
        else:
            dim.resolve(data_model)
            dimensions.append(dim)

    # Resolve metrics
    metrics = []
    for metric_name in biquery.metrics:
        metric = semantic_model.get_metric(metric_name)

        if metric is None:
            errors.append(DataChainError(
                stage="resolution",
                code="metric_not_found",
                message=f"Metric '{metric_name}' not found in semantic model."
            ))
        else:
            metric.resolve(data_model, semantic_model)
            metrics.append(metric)

    # Resolve filters
    filters = []
    for filter_name in biquery.filters:
        filter_obj = semantic_model.get_filter(filter_name)

        if filter_obj is None:
            errors.append(DataChainError(
                stage="resolution",
                code="filter_not_found",
                message=f"Filter '{filter_name}' not found in semantic model."
            ))
        else:
            filter_obj.resolve(data_model, semantic_model)
            filters.append(filter_obj)

    # Resolve metric filters
    metric_filters = []
    for metric_filter_name in biquery.metric_filters:
        metric_filter_obj = semantic_model.get_filter(metric_filter_name)

        if metric_filter_obj is None:
            errors.append(DataChainError(
                stage="resolution",
                code="metric_filter_not_found",
                message=f"Metric filter '{metric_filter_name}' not found in semantic model."
            ))
        else:
            metric_filter_obj.resolve(data_model, semantic_model)
            metric_filters.append(metric_filter_obj)

    # Resolve orderby
    orderby = []
    for col, direction in biquery.orderby:
        col_obj = semantic_model.get_dimension(col) or semantic_model.get_metric(col)

        if col_obj is None:
            errors.append(DataChainError(
                stage="resolution",
                code="orderby_column_not_found",
                message=f"Orderby column '{col}' not found."
            ))
        elif direction not in ("asc", "desc"):
            errors.append(DataChainError(
                stage="resolution",
                code="invalid_sorting_direction",
                message=f"Invalid sorting direction '{direction}'."
            ))
        else:
            orderby.append((col_obj, direction))

    resolved_query = ResolvedQuery(
        dimensions=dimensions,
        metrics=metrics,
        filters=filters,
        metric_filters=metric_filters,
        orderby=orderby,
        limit=biquery.limit,
        offset=biquery.offset,
        distinct=biquery.distinct

    )

    return ResolutionResult(
        success=len(errors) == 0,
        resolved_query=resolved_query,
        errors=errors
    )
