from .relationship import Relationship
from .table import TableModel

TABLE_REGISTRY: dict[str, TableModel] = {}
RELATIONSHIP_REGISTRY: list[Relationship] = []