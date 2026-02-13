from .models import TableModel, Relationship

class DataModel():
    def __init__(
        self,
    ):
        self._tables: dict[str, TableModel] = {}
        self._relationships: list[Relationship] = []

    def get_table(self, name: str) -> TableModel | None:
        return self._tables.get(name)
    
    def register_table(self, table: TableModel):
        setattr(self, table.name, table.ibis())
        self._tables[table.name] = table

    def register_relationship(self, relationship: Relationship):
        self._relationships.append(relationship)

    def get_relationship_graph(self, directed: bool = True) -> dict[TableModel, list[Relationship]]:
        graph = {table: [] for table in self._tables.values()}
        for rel in self._relationships:
            graph[rel.left].append(rel)
            if not directed:
                graph[rel.right].append(rel)
        return graph