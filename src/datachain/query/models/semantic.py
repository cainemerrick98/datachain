from pydantic import BaseModel, model_validator, ValidationError
from enum import Enum
from typing import Optional, Union, Literal
from collections import defaultdict
from .enums import Aggregation, Comparator, Arithmetic

# Enums

class DataType(Enum):
    DATE = "DATE"
    STRING = "STRING"
    NUMERIC = "NUMERIC"
    BOOLEAN = "BOOLEAN"


class RelationshipType(Enum):
    ONE_TO_MANY = "ONE_TO_MANY"


# Models

class SemanticColumn(BaseModel):
    name: str
    type: DataType
    description: str


class Table(BaseModel):
    name: str
    columns: list[SemanticColumn]
    description: str


class Relationship(BaseModel):
    """
    Example:
    incoming = "Customer"
    type = "ONE_TO_MANY"
    outgoing = "Order"

    Customer 1 -> n Order
    """
    incoming: str
    keys_incoming: list[str]
    type: RelationshipType
    outgoing: str
    keys_outgoing: list[str]


class SemanticMetric(BaseModel):
    table: str
    column: str
    aggregation: Aggregation


#TODO: Add validation that left and right refer to valid KPI names
class SemanticBinaryMetric(BaseModel):
    left: str # Must refer to a KPI name
    operator: Arithmetic
    right: str # Must refer to a KPI name

#TODO: add display name to KPI (maybe formatting aswell)
class KPI(BaseModel):
    name: str
    expression: Union[SemanticMetric, SemanticBinaryMetric]
    description: str
    return_type: DataType


class SemanticComparison(BaseModel):
    table: str
    column: str
    comparator: Comparator
    value: Union[str, float, int, bool, list[str]]


class SemanticKPIComparison(BaseModel):
    name: str
    kpi: KPI
    comparator: Comparator
    value: Union[str, float, int, bool]


class Filter(BaseModel):
    name: str
    predicate: Union[SemanticComparison, SemanticKPIComparison]
    description: str


class SemanticModel(BaseModel):
    tables: list[Table]
    kpis: Optional[list[KPI]] = None
    filters: Optional[list[Filter]] = None
    relationships: Optional[list[Relationship]] = None

    @model_validator(mode='after')
    def validate_relationships(self):

        if self.relationships is None:
            return self

        #Validate the relationships are all in the tables and columns list
        field_names = [(t.name, c.name) for t in self.tables for c in t.columns]
        errors = []
        for r_ind, relationship in enumerate(self.relationships):
            if len([f for f in field_names if f[0] == relationship.incoming]) == 0:
                errors.append({
                        "type": "value_error",
                        "loc": ("relationships", r_ind, "incoming"),
                        "msg": (
                            f"incoming must reference a table in tables"
                        ),
                        "input": relationship.incoming,
                        "ctx": {"error": "invalid_table_reference"},
                    })
            if len([f for f in field_names if f[0] == relationship.outgoing]) == 0:
                errors.append({
                        "type": "value_error",
                        "loc": ("relationships", r_ind, "outgoing"),
                        "msg": (
                            f"incoming must reference a table in tables"
                        ),
                        "input": relationship.incoming,
                        "ctx": {"error": "invalid_table_reference"},
                    })
            
            for k_ind, key in enumerate(relationship.keys_incoming):
                if (relationship.incoming, key) not in field_names:
                    errors.append({
                        "type": "value_error",
                        "loc": ("relationships", r_ind, "keys_incoming", k_ind),
                        "msg": (
                            f"incoming keys must reference a valid table.column pair"
                        ),
                        "input": relationship.keys_incoming[k_ind],
                        "ctx": {"error": "invalid_table_column_reference"},
                    })
            
            for k_ind, key in enumerate(relationship.keys_outgoing):
                if (relationship.outgoing, key) not in field_names:
                    errors.append({
                        "type": "value_error",
                        "loc": ("relationships", r_ind, "keys_outgoing", k_ind),
                        "msg": (
                            f"Outgoing keys must reference a valid table.column pair"
                        ),
                        "input": relationship.keys_outgoing[k_ind],
                        "ctx": {"error": "invalid_table_column_reference"},
                    })

        # Validate the relationship graph have no seperation and there are no cycles
        # Cycles are checked via DFS and a recursion stack

        tables = [t.name for t in self.tables]
        visited = set()
        rec_stack = set()
        directed_graph = self.get_relationship_graph()

        def is_cyclic(node: str, visited: set, rec_stack: set, graph: dict[str, list[str]]) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if is_cyclic(neighbor, visited, rec_stack, graph):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        for table in tables:
            if table in visited:
                continue
            if is_cyclic(table, visited, rec_stack, directed_graph):
                errors.append({
                    "type": "value_error",
                    "loc": ("relationships",),
                    "msg": (
                        f"The graph contains a cycle at table: {table}"
                    ),
                    "input": self.relationships,
                    "ctx": {"error": "Cyclic graph"},
                })
                break

        
        # Check for weak connectivity by ensuring all nodes are reachable from an arbitrary starting node
        visited = set()
        undirected_graph = self.get_relationship_graph(directed=False)
        nodes_to_check = [tables[0]]
        while nodes_to_check:
            node = nodes_to_check.pop()
            if node in visited:
                continue
            visited |= {node}
            nodes_to_check += [n for n in undirected_graph[node] if n not in visited]
        
        if len(visited) != len(tables):
            errors.append({
                "type": "value_error",
                "loc": ("relationships",),
                "msg": (
                    f"The graph is disconnected"
                ),
                "input": self.relationships,
                "ctx": {"error": "Disconnected graph"},
            })

        if errors:
            raise ValidationError.from_exception_data(
                self.__class__.__name__,
                errors,
            )
        
        return self

    @property
    def fields(self):
        return {t.name: {c.name: c.type for c in t.columns} for t in self.tables}
    
    def field_exists(self, table: str, column: str) -> bool:
        return table in self.fields and column in self.fields[table]
    
    def is_correct_type(self, table: str, column: str, type_: DataType) -> bool:
        return isinstance(self.fields[table][column], type_)
        
    def _get_entity(self, entity_type: Literal["kpis", "filters"], name: str) -> Union[KPI, Filter, None]:
        entities = getattr(self, entity_type)

        matches = [ent for ent in entities if name==ent.name]
        if len(matches) == 1:
            return matches[0]
        
        return None

    def get_kpi(self, name) -> KPI:
        return self._get_entity("kpis", name)
    
    def get_filter(self, name) -> Filter:
        return self._get_entity("filters", name)

    def get_relationship_graph(self, directed: bool = True) -> dict[str, list[str]] | None:
        if self.relationships is None:
            return None
        
        graph = defaultdict(list)

        # First initialize all tables in the graph
        for table in self.tables:
            graph[table.name] = []

        # Then add edges
        for relationship in self.relationships:
            graph[relationship.incoming].append(relationship.outgoing)
            if not directed:
                graph[relationship.outgoing].append(relationship.incoming)
        
        return graph

