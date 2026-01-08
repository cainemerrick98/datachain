from pydantic import BaseModel
from enum import Enum
from typing import Optional, Union, Literal
from sql import MetricExpr, Predicates

# Errors
class DuplicateSemanticModelEntityError(Exception):
    pass

class MissingSemanticModelEntityError(Exception):
    pass


# Enums

class DataTypes(Enum):
    DATE = "DATE"
    STRING = "STRING"
    NUMERIC = "NUMERIC"
    BOOLEAN = "BOOLEAN"


class SemanticColumn(BaseModel):
    name: str
    data_type: DataTypes
    description: str


class Table(BaseModel):
    name: str
    columns: list[SemanticColumn]
    description: str


class KPI(BaseModel):
    name: str
    expression: MetricExpr
    description: str
    return_type: DataTypes


class Filter(BaseModel):
    name: str
    predicate: Predicates
    description: str


class SemanticModel(BaseModel):
    tables: list[Table]
    kpis: Optional[list[KPI]] = None
    filters: Optional[list[Filter]] = None

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


