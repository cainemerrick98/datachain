import ibis.expr.types as ir
from .data_model import DataModel, TableModel, Relationship, ColumnType
from typing import Callable
import ibis.expr.types as ir
from .semantic_model import SemanticModel, Metric, Dimension, Filter

class ModelBuilder:
    def __init__(self):
        self._data_model = DataModel()
        self._semantic_model = SemanticModel()

    @property
    def data_model(self) -> DataModel:
        return self._data_model

    def table(self, name: str) -> TableModel:
        """Decorator to create a table model from a function."""
        def decorator(func: Callable[[], dict[str, ColumnType]]) -> TableModel:
            schema = func()
            table_model = TableModel(name=name, schema=schema)
            self._data_model.register_table(table_model)
            return table_model
        return decorator

    def relationship(self, left: TableModel, right: TableModel, how: str = "left"):
        """Decorator to create a relationship between two table models."""
        def decorator(
            func: Callable[[ir.Table, ir.Table], ir.BooleanValue]
        ) -> Relationship:
            rel = Relationship(left=left, right=right, on=func, how=how)
            self._data_model.register_relationship(rel)
            return rel
        return decorator

    def metric(self, name: str, grain: str, dependencies: list[Metric] | None = None):
        """Decorator to create a metric from a function."""
        def decorator(func: Callable[[DataModel, SemanticModel], ir.Value]) -> Metric:
            deps = dependencies or []
            metric = Metric(name=name, grain=grain, dependencies=deps, expression=func)
            self._semantic_model.register_metric(metric)
            return metric
        return decorator

    def dimension(self,name: str):
        """Decorator to create a dimension from a function."""
        def decorator(func: Callable[[DataModel], ir.Expr]) -> Dimension:
            dimension = Dimension(name=name, expression=func)
            self._semantic_model.register_dimension(dimension)
            return dimension
        return decorator


    def filter(self, name: str):
        """Decorator to create a filter from a function."""
        def decorator(func: Callable[[DataModel, SemanticModel], ir.BooleanValue]) -> Filter:
            filter = Filter(name=name, expression=func)
            self._semantic_model.register_filter(filter)
            return filter
        return decorator