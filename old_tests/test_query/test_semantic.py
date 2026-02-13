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
                incoming="Order",
                keys_incoming=["customer_id"],
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
                incoming="Order",
                keys_incoming=["customer_id"],
                outgoing="Customer",
                keys_outgoing=["ID"],
                type=RelationshipType.ONE_TO_MANY
            ),
            Relationship(
                incoming="Order",
                keys_incoming=["plant_id"],
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
                    incoming="Orders", # Order incorrectly spelt
                    keys_incoming=["customer_id"],
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
                    incoming="Order", 
                    keys_incoming=["customers_id"], # customer_id incorrectly spelt
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                )
            ]
        )

def test_invalid_graph_disconnected():
    with pytest.raises(ValidationError):
        model = SemanticModel(
            tables=[order, customer, plant],
            relationships=[
                Relationship(
                    incoming="Order",
                    keys_incoming=["customer_id"],
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                )
                # Plant table is disconnected
            ]
        )

def test_invalid_graph_disconnected_multiple():
    with pytest.raises(ValidationError):
        model = SemanticModel(
            tables=[order, customer, plant, plant_group],
            relationships=[
                Relationship(
                    incoming="Order",
                    keys_incoming=["customer_id"],
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                ),

                #Plant and PlantGroup tables are disconnected from Order and Customer
                Relationship(
                    incoming="PlantGroup",
                    keys_incoming=["ID"],
                    outgoing="Plant",
                    keys_outgoing=["plant_group_id"],
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
                    incoming="Order",
                    keys_incoming=["ID"],
                    outgoing="Order",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incoming="Order", 
                    keys_incoming=["customer_id"],
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
                    incoming="Order",
                    keys_incoming=["customer_id"],
                    outgoing="Customer",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incoming="Customer",
                    keys_incoming=["ID"],
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
                    incoming="Customer",
                    keys_incoming=["ID"],
                    outgoing="Order",
                    keys_outgoing=["customer_id"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incoming="Order",
                    keys_incoming=["plant_id"],
                    outgoing="Plant",
                    keys_outgoing=["ID"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incoming="Plant",
                    keys_incoming=["ID"],
                    outgoing="Customer",
                    keys_outgoing=["some_column"],
                    type=RelationshipType.ONE_TO_MANY
                ),
                Relationship(
                    incoming="PlantGroup",
                    keys_incoming=["ID"],
                    outgoing="Plant",
                    keys_outgoing=["plant_group_id"],
                    type=RelationshipType.ONE_TO_MANY
                )
            ]
        )