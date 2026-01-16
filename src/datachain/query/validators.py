from .models import SemanticModel, BIQuery
from .models import DataType

def validate_biquery_agaisnt_semantic_model(biquery: BIQuery, semantic_model: SemanticModel) -> tuple[bool, list[dict]]:
    """
    Responsible for checking if a biquery is valid given a semantic model 
    and if not collecting the errors to pass to pass to the LLM
    """

    is_valid = True

    valid_fields = set()
    valid_fields |= {f"{table.name}.{column.name}" for table in semantic_model.tables for column in table.columns}
    valid_fields |= {kpi.name for kpi in semantic_model.kpis}
    valid_fields |= {_filter.name for _filter in semantic_model.filters}
    
    # Validate that all referenced fields are in the semantic model
    errors = []
    #TODO update filters in referenced fields set
    referenced_fields = {d.ref for d in biquery.dimensions} | {k for k in biquery.kpi_refs} | {m.ref for m in biquery.measures} | {f for f in biquery.filter_refs} | {f.field for f in biquery.dimension_filters}
    for field in referenced_fields:
        if field not in valid_fields:
            is_valid = False
            # TODO: add more informative error e.g. where is the invalid field in the biquery
            errors.append({
                "type": "invalid_field",
                "msg": f"field: {field} is not in the semantic model"
            })

    # Validate that if time grain is applied, the dimension is a date type
    date_columns = {f"{table.name}.{column.name}" for table in semantic_model.tables for column in table.columns if column.type == DataType.DATE}
    for dimension in biquery.dimensions:
        if dimension.time_grain is not None:
            if dimension.ref not in date_columns:
                is_valid = False
                errors.append({
                    "type": "invalid_time_grain",
                    "msg": f"dimension: {dimension.ref} has time grain applied but is not a date type"
                })

    # Validate join path is valid
    # A valid join path means that there are no 1 -> n -> 1 relationships in the path
    """
    Example of an invalid join path:
    Order(n) -> Customer(1)
    Customer(1) -> SalesRep(n)

    invalid query:
    SELECT AVG(SalesRep.Commission), Customer.Region
    FROM Order
    JOIN Customer ON Order.CustomerID = Customer.ID
    JOIN SalesRep ON Order.SalesRepID = SalesRep.ID

    reasoning:
    We try to aggregate
    """

    """

    Example of a valid join path:
    Order(n) -> SalesRep(1)
    SalesRep(n) -> Region(1)

    valid query:
    SELECT AVG(SalesRep.Commission), Region.Name
    FROM SalesRep
    JOIN SalesRep ON Order.SalesRepID = SalesRep.ID

    reasoning:
    There is a direct relationship between SalesRep and Region. 
    **The path is 1 -> n -> 1**
    """
    graph = semantic_model.get_relationship_graph()
    if graph is not None:
        # TODO: we do need to implement this check - but later
        pass
        

    return is_valid, errors