from models import SemanticModel, BIQuery

class BIValidationError(Exception):
    pass

def validate_biquery_agaisnt_semantic_model(biquery: BIQuery, semantic_model: SemanticModel):

    valid_fields = set()
    
    # Validate that all referenced fields are in the semantic model
    # We dont check the filters as they have already been validated to reference one of the below
    referenced_fields = [d.ref for d in biquery.dimensions] + [k for k in biquery.kpi_refs] + [m.ref for m in biquery.measures]
    for field in referenced_fields:



    # Validate that all kpis and filters refs are in the semantic model

