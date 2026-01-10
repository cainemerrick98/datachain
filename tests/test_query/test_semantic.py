"""
To test the validation logic on the SemanticModel class
"""
import pytest
from pydantic import ValidationError
from src.datachain.query.models import (
    SemanticModel,
    Table,
    SemanticColumn,
    Relationship,
    RelationshipType,
    DataType
)


order = Table(
    name="Order",
    columns=[
        SemanticColumn(
            name="ID",
            type=DataType.STRING,
            description='None'
        ),
        SemanticColumn(
            name="customer_id",
            type=DataType.STRING,
            description='None'
        ),
        SemanticColumn(
            name="plant_id",
            type=DataType.STRING,
            description='None'
        ),
    ],
    description='None'
)

customer = Table(
    name="Customer",
    columns=[
        SemanticColumn(
            name="ID",
            type=DataType.STRING,
            description='None'
        ),
        SemanticColumn(
            name="some_column",
            type=DataType.STRING,
            description='None'
        ),
    ],
    description='None'
)

plant = Table(
    name="Plant",
    columns=[
        SemanticColumn(
            name="ID",
            type=DataType.STRING,
            description='None'
        ),
        SemanticColumn(
            name="plant_group_id",
            type=DataType.STRING,
            description='None'
        ),
    ],
    description='None'
)

plant_group = Table(
    name="PlantGroup",
    columns=[
        SemanticColumn(
            name="ID",
            type=DataType.STRING,
            description='None'
        ),
    ],
    description='None'
)



def test_order_customer_valid():
    model = SemanticModel(
        tables=[order, customer],
        relationships=[
            Relationship(
                incomming="Order",
                keys_incomming=["customer_id"],
                outgoing="Customer",
                keys_outgoing=["ID"],
                type=RelationshipType.ONE_TO_MANY
            )
        ],
    )


def test_valid_full_model():
    model = SemanticModel(
        tables=[order, customer, plant],
        relationships=[
            Relationship(
                incomming="Order",
                keys_incomming=["customer_id"],
                outgoing="Customer",
                keys_outgoing=["ID"],
                type=RelationshipType.ONE_TO_MANY
            ),
            Relationship(
                incomming="Order",
                keys_incomming=["plant_id"],
                outgoing="Plant",
                keys_outgoing=["ID"],
                type=RelationshipType.ONE_TO_MANY
            )
        ]
    )

def test_invalid_table_reference():
    with pytest.raises(ValidationError):
        model = SemanticModel(
            tables=[order, customer],
            relationships=[
                Relationship(
                    incomming="Orders", # Order incorrectly spelt
                    keys_incomming=["customer_id"],
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                )
            ]
        )

def test_invalid_column_reference():
    with pytest.raises(ValidationError):
        model = SemanticModel(
            tables=[order, customer],
            relationships=[
                Relationship(
                    incomming="Order", 
                    keys_incomming=["customers_id"], # customer_id incorrectly spelt
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                )
            ]
        )

def test_invalid_contains_loop():
    with pytest.raises(ValidationError):
        model = SemanticModel(
            tables=[order, customer],
            relationships=[
                Relationship(
                    incomming="Order",
                    keys_incomming=["ID"],
                    outgoing="Order",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incomming="Order", 
                    keys_incomming=["customer_id"],
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                )
            ]
        )

def test_invalid_contains_cycle():
    with pytest.raises(ValidationError):
        model = SemanticModel(
            tables=[order, customer],
            relationships=[
                Relationship(
                    incomming="Order",
                    keys_incomming=["customer_id"],
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incomming="Customer",
                    keys_incomming=["ID"],
                    outgoing="Order",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                )
            ]
        )

def test_invalid_contains_cycle_complex():
    with pytest.raises(ValidationError):
        model = SemanticModel(
            tables=[order, customer, plant, plant_group],
            relationships=[
                Relationship(
                    incomming="Customer",
                    keys_incomming=["ID"],
                    outgoing="Order",
                    keys_outgoing=["customer_id"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incomming="Plant",
                    keys_incomming=["ID"],
                    outgoing="Order",
                    keys_outgoing=["plant_id"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incomming="Plant",
                    keys_incomming=["ID"],
                    outgoing="Customer",
                    keys_outgoing=["some_column"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incomming="PlantGroup",
                    keys_incomming=["ID"],
                    outgoing="Plant",
                    keys_outgoing=["plant_group_id"],
                    type=RelationshipType.ONE_TO_MANY
                )
            ]
        )