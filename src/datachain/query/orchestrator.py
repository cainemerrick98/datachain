from .models import BIQuery, SemanticModel, ResolvedBIQuery, SQLQuery
from .validator import QueryValidator
from .planner import QueryPlanner
from .resolver import QueryResolver
from dataclasses import dataclass, field
from typing import Optional, Any

@dataclass
class QueryContext:
    # Discovered during resolution
    tables: set[str] = field(default_factory=set)
    common_table: Optional[str] = None

    # Discovered during planning
    joins: list = field(default_factory=list)
    requires_subquery: bool = False
    requires_cte: bool = False

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
        
        resolved_query = self.resolver.lower(bi_query, ctx)

        join_path_errors = self.validator.validate_join_path(resolved_query, self.semantic_model, ctx)
        if join_path_errors:
            return QueryResult(
                None, join_path_errors, ctx
            )
        
        self.planner.analyse_context(ctx, self.semantic_model)
        sql_query = self.planner.plan(resolved_query, ctx)

        return QueryResult(sql_query, [], ctx)
 



        



