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
    """
    Example of an invalid join path:
    Order(n) -> Customer(1)
    Customer(1) -> SalesRep(n)

    SELECT AVG(Order.Value), SalesRep.Name
    FROM SalesRep
    JOIN Customer ON Order.CustomerID = Customer.ID
    JOIN SalesRep ON SalesRep.CustomerID = Customer.ID
    GROUP BY SalesRep.Name
  
    reasoning:
    Invalid because there is a 1 -> n -> 1 relationship in the path (Customer is on the 1 side of both relationships).
    In this example we want the average order value per sales rep. However, because a sales rep can have many customers,
    and a customer can have many orders, the average order value per sales rep is not well defined. We have to first aggregate
    the order values per customer, then aggregate those per sales rep. 

    Simply put we cannot assign individual sales orders to sales reps
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
        """
        For each table in the biquery we follow the relationships to find the n most side
        If there is disagreement this means this query cannot be executed as we have two tables that do not share a common table.
        """
        
        

    return is_valid, errors