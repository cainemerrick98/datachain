from .models import BIQuery, ResolvedBIQuery, BIMeasure, BIFilter, ResolvedBIMeasureFilter, ResolvedBIFilter
from .models import SemanticModel, KPI, Filter, SemanticComparison, SemanticKPIComparison
from typing import Literal

def resolve_bi_query(biquery: BIQuery, semantic_model: SemanticModel) -> ResolvedBIQuery:
    resolved_filters = resolve_semantic_filters(biquery.filter_refs, semantic_model)

    return ResolvedBIQuery(
        dimensions=biquery.dimensions,
        measures=biquery.measures + [resolve_kpi(name, semantic_model) for name in biquery.kpi_refs],
        measure_filters=[resolve_measure_filter(mes_filt) for mes_filt in biquery.measure_filters] + resolved_filters["measure_filters"],
        dimension_filters=[resolve_dimension_filter(dim_filter) for dim_filter in biquery.dimension_filters] + resolved_filters["dimension_filters"],
        order_by=biquery.order_by,
        limit=biquery.limit
    )


def resolve_semantic_filters(names: list[str], semantic_model: SemanticModel) -> dict:
    resolved_filters = {
        "dimension_filters":[],
        "measure_filters":[]
    }
    for name in names:
        semantic_filter = semantic_model.get_filter(name)

        if isinstance(semantic_filter.predicate, SemanticComparison):
            resolved_filters["dimension_filters"].append(
                ResolvedBIFilter(
                    table=semantic_filter.predicate.table,
                    column=semantic_filter.predicate.column,
                    comparator=semantic_filter.comparator,
                    value=semantic_filter.predicate.value
                )
            )
        else:
            resolved_filters["measure_filters"].append(
                ResolvedBIMeasureFilter(
                    measure=resolve_kpi(
                        semantic_filter.predicate.kpi,
                        semantic_model
                    ),
                    comparator=semantic_filter.predicate.comparator,
                    value=semantic_filter.predicate.value
                )
            )
    return resolved_filters


def resolve_kpi(name: str, semantic_model: SemanticModel) -> BIMeasure:
    kpi = semantic_model.get_kpi(name)
    return BIMeasure(
        name=kpi.name,
        table=kpi.expression.table,
        column=kpi.expression.column,
        aggregation=kpi.expression.aggregation
    )

def resolve_measure_filter(mes_filter: BIFilter, resolved_query: ResolvedBIQuery) -> ResolvedBIMeasureFilter:
    #TODO has it been validated that these exist?
    measure = next(m for m in resolved_query.measures if m.name == mes_filter.field)
    return ResolvedBIMeasureFilter(
        measure=measure,
        comparator=mes_filter.comparator,
        value=mes_filter.value
    )


def resolve_dimension_filter(dim_filter: BIFilter) -> ResolvedBIFilter:
    return ResolvedBIFilter(
        table=dim_filter.table,
        column=dim_filter.column,
        comparator=dim_filter.comparator,
        value=dim_filter.value
    )

