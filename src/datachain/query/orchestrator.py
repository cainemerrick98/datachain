from .models import BIQuery, SemanticModel, ResolvedBIQuery, SQLQuery
from .validator import QueryValidator
from .planner import QueryPlanner
from .resolver import QueryResolver
from .types import QueryContext, QueryResult


class QueryOrchestrator():
    def __init__(self, semantic_model: SemanticModel):
        self.semantic_model = semantic_model
        self.validator = QueryValidator()
        self.resolver = QueryResolver()
        self.planner = QueryPlanner()

    def run(self, bi_query: BIQuery) -> QueryResult:
        ctx = QueryContext()

        struct_errors = self.validator.validate_structure(bi_query, ctx)
        if struct_errors:
            return QueryResult(
                None, struct_errors, ctx
            )
        
        ref_errors = self.validator.validate_references(bi_query, self.semantic_model, ctx)
        if ref_errors:
            return QueryResult(
                None, ref_errors, ctx
            )
        
        # ctx is updated with all tables in the query
        resolved_query = self.resolver.resolve(bi_query, ctx)

        join_path_errors = self.validator.validate_join_path(resolved_query, self.semantic_model, ctx)
        if join_path_errors:
            return QueryResult(
                None, join_path_errors, ctx
            )
        
        self.planner.analyse_context(ctx, self.semantic_model)
        sql_query = self.planner.plan(resolved_query, ctx)

        return QueryResult(sql_query, [], ctx)
 



        



