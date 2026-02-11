from .models import TableModel, Relationship

TABLE_REGISTRY: dict[str, TableModel] = {}
RELATIONSHIP_REGISTRY: list[Relationship] = []