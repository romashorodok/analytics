from data_store.cube import Cube
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    TimestampType,
    DoubleType,
    StringType,
    NestedField,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

from data_store.cube import Dimension

from . import identifier as id


class OrderItem:
    identifier = f"{id}.order_items"

    schema = Schema(
        NestedField(
            field_id=1,
            name="product_id",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=2,
            name="shipping_limit_date",
            field_type=TimestampType(),
            required=False,
        ),
        NestedField(
            field_id=3,
            name="order_id",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=4,
            name="order_item_id",
            field_type=LongType(),
            required=False,
        ),
        NestedField(
            field_id=5,
            name="seller_id",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=6,
            name="price",
            field_type=DoubleType(),
            required=False,
        ),
        NestedField(
            field_id=7,
            name="freight_value",
            field_type=DoubleType(),
            required=False,
        ),
    )

    partition = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=3,
            name="order_id",
            transform=IdentityTransform(),
            required=False,
        ),
    )

    @classmethod
    def new_cube(cls) -> Cube:
        return Cube(
            {
                cls.identifier: Dimension(cls.identifier, cls.schema, cls.partition),
            },
        )
