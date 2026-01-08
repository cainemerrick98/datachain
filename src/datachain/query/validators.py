from models import SemanticModel, BIQuery

class BIValidationError(Exception):
    pass

def validate_biquery_agaisnt_semantic_model(biquery: BIQuery, semantic_model: SemanticModel):
    ...

