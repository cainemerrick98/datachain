import ibis.expr.types as ir
from data_model import TableModel, Relationship

class ModelContext:
    def __init__(
        self,
        tables: dict[str, TableModel],
        relationships: list[Relationship],
    ):
        self._tables = tables
        self._relationships = relationships

        for name, table_model in tables.items():
            setattr(self, name, table_model.ibis())

    def table(self, table: TableModel) -> ir.Table:
        return table.ibis()

    def join(
        self,
        left: TableModel,
        right: TableModel,
    ) -> ir.Table:
        for rel in self._relationships:
            if rel.left == left and rel.right == right:
                l = left.ibis()
                r = right.ibis()
                return l.join(r, rel.on(l, r), how=rel.how)

        raise ValueError(
            f"No relationship between {left.name} and {right.name}"
        )
    
