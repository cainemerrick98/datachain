from pydantic import BaseModel, model_validator, ValidationError
from enum import Enum
from typing import Optional, Union, Literal
from collections import defaultdict
from .enums import Aggregation, Comparator

# Errors
class DuplicateSemanticModelEntityError(Exception):
    pass

class MissingSemanticModelEntityError(Exception):
    pass


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
    incomming = "Customer"
    type = "ONE_TO_MANY"
    outgoing = "Order"

    Customer 1 -> n Order
    """
    incomming: str
    keys_incomming: list[str]
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
    operator: str
    right: str # Must refer to a KPI name


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
            print('No relationships to validate')
            return self

        #Validate the relationships are all in the tables and columns list
        field_names = [(t.name, c.name) for t in self.tables for c in t.columns]
        errors = []
        for r_ind, relationship in enumerate(self.relationships):
            if len([f for f in field_names if f[0] == relationship.incomming]) == 0:
                errors.append({
                        "type": "value_error",
                        "loc": ("relationships", r_ind, "incomming"),
                        "msg": (
                            f"Incomming must reference a table in tables"
                        ),
                        "input": relationship.incomming,
                        "ctx": {"error": "invalid_table_reference"},
                    })
            if len([f for f in field_names if f[0] == relationship.outgoing]) == 0:
                errors.append({
                        "type": "value_error",
                        "loc": ("relationships", r_ind, "outgoing"),
                        "msg": (
                            f"Incomming must reference a table in tables"
                        ),
                        "input": relationship.incomming,
                        "ctx": {"error": "invalid_table_reference"},
                    })
            
            for k_ind, key in enumerate(relationship.keys_incomming):
                if (relationship.incomming, key) not in field_names:
                    errors.append({
                        "type": "value_error",
                        "loc": ("relationships", r_ind, "keys_incomming", k_ind),
                        "msg": (
                            f"Incomming keys must reference a valid table.column pair"
                        ),
                        "input": relationship.keys_incomming[k_ind],
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
        # Cycles are checked via BFS
        tables = [t.name for t in self.tables]
        graph = self.get_relationship_graph()
        for table in tables:
            visited = {table}
            nodes_to_check = [] + graph[table]
            while nodes_to_check:
                node = nodes_to_check.pop()
                if node in visited:
                    errors.append({
                        "type": "value_error",
                        "loc": ("relationships",),
                        "msg": (
                            f"The graph contains a cycle at table: {node}"
                        ),
                        "input": self.relationships,
                        "ctx": {"error": "Cyclic graph"},
                    })
                    break
                visited |= {node}
                nodes_to_check += [n for n in graph[node] if n not in visited]

        
        # Check for disconnected graph
        visited = set()
        nodes_to_check = [tables[0]]
        while nodes_to_check:
            node = nodes_to_check.pop()
            if node in visited:
                continue
            visited |= {node}
            nodes_to_check += [n for n in graph[node]]
        if len(visited) < len(tables):
            errors.append({
                "type": "value_error",
                "loc": ("relationships",),
                "msg": (
                    f"The graph is disconnected"
                ),
                "input": self.relationships,
                "ctx": {"error": "Disconnected graph"},
            })
        print('\n\n')
        print(errors)
        if errors:
            raise ValidationError.from_exception_data(
                self.__class__.__name__,
                errors,
            )
        
        return self
        

    def _get_entity(self, entity_type: Literal["kpis", "filters"], name: str) -> Union[KPI, Filter]:
        entities = getattr(self, entity_type)

        if not(entities):
            raise MissingSemanticModelEntityError(f"No {entity_type} defined in semantic model")

        matches = [ent for ent in entities if name==ent.name]
        if len(matches) == 1:
            return matches[0]
        
        if len(matches) > 1:
            raise DuplicateSemanticModelEntityError(f"More than 1 mantching {entity_type} for {entity_type}: {name}")
        else:
            raise MissingSemanticModelEntityError(f"No matching {entity_type} for {entity_type}: {name}")

    def get_kpi(self, name) -> KPI:
        return self._get_entity("kpis", name)
    
    def get_filter(self, name) -> Filter:
        return self._get_entity("filters", name)

    def get_relationship_graph(self) -> dict[str, list[str]] | None:
        if self.relationships is None:
            return None
        
        graph = defaultdict(list)

        for relationship in self.relationships:
            graph[relationship.incomming].append(relationship.outgoing)
            graph[relationship.outgoing].append(relationship.incomming)
        
        return graph

