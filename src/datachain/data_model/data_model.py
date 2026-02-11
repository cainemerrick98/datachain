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

    def get_relationship_graph(self, directed: bool = False) -> dict[str, list[Relationship]]:
        graph = {table_name: [] for table_name in self._tables.keys()}
        for rel in self._relationships:
            graph[rel.left.name].append(rel)
            if not directed:
                graph[rel.right.name].append(rel)
        return graph
    
data_model = DataModel()