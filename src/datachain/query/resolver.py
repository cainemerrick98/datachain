from .models import (
    BIQuery, ResolvedBIQuery, BIMeasure, BIFilter, 
    ResolvedBIMeasureFilter, ResolvedBIFilter,
    SemanticModel, SemanticComparison, SemanticKPIComparison,
    BIOrderBy, ResolvedOrderByDimension, ResolvedOrderByMeasure
)
from .orchestrator import QueryContext

class QueryResolver:
    def resolve(self, bi_query: BIQuery, semantic_model: SemanticModel, ctx: QueryContext) -> ResolvedBIQuery:
        """
        Fully resolves a BIQuery into a ResolvedBIQuery.
        Updates the context with all tables referenced and tracks diagnostics.
        """
        ctx.trace.append("Resolving BIQuery into ResolvedBIQuery")
        
        # Resolve KPIs into measures
        resolved_measures = list(bi_query.measures)
        for kpi_name in bi_query.kpi_refs:
            kpi_measure = self._resolve_kpi(kpi_name, semantic_model, ctx)
            resolved_measures.append(kpi_measure)

        # Resolve semantic filters
        resolved_filters = self._resolve_semantic_filters(bi_query.filter_refs, semantic_model, ctx)

        # Resolve measure filters
        resolved_measure_filters = [self._resolve_measure_filter(mf, resolved_measures, ctx) for mf in bi_query.measure_filters]
        resolved_measure_filters.extend(resolved_filters["measure_filters"])

        # Resolve dimension filters
        resolved_dimension_filters = [self._resolve_dimension_filter(df, ctx) for df in bi_query.dimension_filters]
        resolved_dimension_filters.extend(resolved_filters["dimension_filters"])

        # Resolve order by
        resolved_order_bys = [self._resolve_order_by(order_by, bi_query) for order_by in bi_query.order_by]

        # Update context with tables from dimensions
        for dim in bi_query.dimensions:
            ctx.tables.add(dim.table)
        for m in resolved_measures:
            ctx.tables.add(m.table)
        for f in resolved_dimension_filters:
            ctx.tables.add(f.table)
        for mf in resolved_measure_filters:
            ctx.tables.add(mf.measure.table)

        return ResolvedBIQuery(
            dimensions=bi_query.dimensions,
            measures=resolved_measures,
            measure_filters=resolved_measure_filters,
            dimension_filters=resolved_dimension_filters,
            order_by=resolved_order_bys,
            limit=bi_query.limit
        )

    def _resolve_kpi(self, name: str, semantic_model: SemanticModel, ctx: QueryContext) -> BIMeasure:
        kpi = semantic_model.get_kpi(name)
        ctx.trace.append(f"Resolved KPI '{name}' to measure: {kpi.expression.table}.{kpi.expression.column}")
        return BIMeasure(
            name=kpi.name,
            table=kpi.expression.table,
            column=kpi.expression.column,
            aggregation=kpi.expression.aggregation
        )

    def _resolve_semantic_filters(self, names: list[str], semantic_model: SemanticModel, ctx: QueryContext) -> dict:
        resolved = {"dimension_filters": [], "measure_filters": []}
        for name in names:
            semantic_filter = semantic_model.get_filter(name)
            ctx.trace.append(f"Resolving semantic filter '{name}'")
            pred = semantic_filter.predicate

            if isinstance(pred, SemanticComparison):
                resolved["dimension_filters"].append(
                    ResolvedBIFilter(
                        table=pred.table,
                        column=pred.column,
                        comparator=pred.comparator,
                        value=pred.value
                    )
                )
            elif isinstance(pred, SemanticKPIComparison):
                measure = self._resolve_kpi(pred.kpi.name, semantic_model, ctx)
                resolved["measure_filters"].append(
                    ResolvedBIMeasureFilter(
                        measure=measure,
                        comparator=pred.comparator,
                        value=pred.value
                    )
                )
        return resolved

    def _resolve_measure_filter(self, mes_filter: BIFilter, resolved_measures: list[BIMeasure], ctx: QueryContext) -> ResolvedBIMeasureFilter:
        # Assumes validation has already confirmed existence
        measure = next((m for m in resolved_measures if m.name == mes_filter.field), None)
        ctx.trace.append(f"Resolved measure filter on '{mes_filter.field}'")
        return ResolvedBIMeasureFilter(
            measure=measure,
            comparator=mes_filter.comparator,
            value=mes_filter.value
        )

    def _resolve_dimension_filter(self, dim_filter: BIFilter, ctx: QueryContext) -> ResolvedBIFilter:
        ctx.trace.append(f"Resolved dimension filter on '{dim_filter.table}.{dim_filter.column}'")
        return ResolvedBIFilter(
            table=dim_filter.table,
            column=dim_filter.column,
            comparator=dim_filter.comparator,
            value=dim_filter.value
        )
    
    def _resolve_order_by(self, order_by: BIOrderBy, bi_query: BIQuery) -> ResolvedOrderByMeasure | ResolvedOrderByDimension:
        if "." in order_by.field:
            table, column = order_by.field.split(".")
            return ResolvedOrderByDimension(
                table=table,
                column=column,
                sorting=order_by.sorting
            )
        
        elif order_by.field.startswith("kpi"):
            measure = self._resolve_kpi(order_by.field)
        
        else:
            measure = next(m for m in bi_query.measures if m.name == order_by.field)
        
        return ResolvedOrderByMeasure(
            measure=measure,
            sorting=order_by.sorting
        )

