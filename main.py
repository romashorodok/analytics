from typing import Any
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, YearTransform
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
import uuid
from datetime import date, datetime, timezone


from seeder import EcommerceSeeder, order_to_dataframe
import pyarrow as pa
import pandas as pd

seeder = EcommerceSeeder()
order = next(seeder.generate_random_orders(1))
df = pd.DataFrame(order_to_dataframe(order))

selected_df = df[["order_purchase_timestamp", "order_delivered_customer_date"]]

table = pa.Table.from_pandas(selected_df)

# table = pa.Table.from_pandas(df)


# print(table)

# data = {
#     "id": [1, 2, 3],
#     "name": ["Alice", "Bob", None],  # 'None' here will cause a null type
#     "amount": [100.5, None, 150.75],  # 'None' here will cause a null type
# }
# df = pd.DataFrame(data)
#
# # Define explicit schema with nullable fields for PyArrow
# schema = pa.schema(
#     [
#         ("id", pa.int64()),  # Non-nullable integer
#         ("name", pa.string()),  # Nullable string
#         ("amount", pa.float64()),  # Nullable float
#     ]
# )

# table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)


pg_conn = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
#
catalog = SqlCatalog(
    "default",
    **{
        "uri": pg_conn,
        "warehouse": "s3://parquet",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.session-token": "",
        "s3.region": "us-west-1",
    },
)
#
#
catalog.create_namespace_if_not_exists("default")
#
# schema = Schema(
#     NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
#     # NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
#     # NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
#     # NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
#     # NestedField(
#     #     field_id=5,
#     #     name="details",
#     #     field_type=StructType(
#     #         NestedField(
#     #             field_id=4, name="created_by", field_type=StringType(), required=False
#     #         ),
#     #     ),
#     #     required=False,
#     # ),
# )
#
#
# partition_spec = PartitionSpec(
#     PartitionField(
#         source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
#     )
# )
#
# # table = catalog.drop_table("default.taxi_dataset")
# table = catalog.create_table(
#     "default.taxi_dataset", schema=schema, partition_spec=partition_spec
# )
#

schema = Schema(
    NestedField(
        field_id=1,
        name="order_purchase_timestamp",
        field_type=TimestampType(),
        required=False,
    ),
    NestedField(
        field_id=2,
        name="order_delivered_customer_date",
        field_type=TimestampType(),
        required=False,
    ),
)

# test: pa.Table = pa.Table.from_pydict(
#     {
#         "bool": [False, None, True],
#         "string": ["a", None, "z"],
#         # Go over the 16 bytes to kick in truncation
#         "string_long": ["a" * 22, None, "z" * 22],
#         "int": [1, None, 9],
#         "long": [1, None, 9],
#         "float": [0.0, None, 0.9],
#         "double": [0.0, None, 0.9],
#         # 'time': [1_000_000, None, 3_000_000],  # Example times: 1s, none, and 3s past midnight #Spark does not support time fields
#         "timestamp": [
#             datetime(2023, 1, 1, 19, 25, 00),
#             None,
#             datetime(2023, 3, 1, 19, 25, 00),
#         ],
#         "timestamptz": [
#             datetime(2023, 1, 1, 19, 25, 00, tzinfo=timezone.utc),
#             None,
#             datetime(2023, 3, 1, 19, 25, 00, tzinfo=timezone.utc),
#         ],
#         "date": [date(2023, 1, 1), None, date(2023, 3, 1)],
#         # Not supported by Spark
#         # 'time': [time(1, 22, 0), None, time(19, 25, 0)],
#         # Not natively supported by Arrow
#         # 'uuid': [uuid.UUID('00000000-0000-0000-0000-000000000000').bytes, None, uuid.UUID('11111111-1111-1111-1111-111111111111').bytes],
#         "binary": [b"\01", None, b"\22"],
#         "fixed": [
#             uuid.UUID("00000000-0000-0000-0000-000000000000").bytes,
#             None,
#             uuid.UUID("11111111-1111-1111-1111-111111111111").bytes,
#         ],
#     },
# )


partition_spec = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1,
        transform=YearTransform(),
        name="order_purchase_timestamp",
    ),
    # PartitionField(
    #     source_id=1,
    #     field_id=1,
    #     transform=DayTransform(),
    #     name="order_purchase_timestamp",
    # ),
)

properties: dict[str, Any] = {}

catalog.drop_table("default.taxi_dataset")
# tbl = catalog.create_table(
#     "default.taxi_dataset",
#     schema=test.schema,
#     properties=properties,
# )

tbl = catalog.create_table_if_not_exists(
    "default.taxi_dataset",
    schema=schema,
    properties=properties,
)

# for _ in range(5):
#     tbl.append(test)
#     tbl.overwrite(test)

# for order in seeder.generate_random_orders(2):
#     df = pd.DataFrame(order_to_dataframe(order))
#     selected_df = df[["order_purchase_timestamp"]]
#     print(df)
#     table: pa.Table = pa.Table.from_pandas(selected_df)
#     _table.append(table)

#
# orders = seeder.generate_random_orders(1000)
#
# for order in orders:
#     df = pd.DataFrame(order_to_dataframe(order))
#     selected_df = df[['order_purchase_timestamp']]
#     table = pa.Table.from_pandas(selected_df)
#     _table.append(table)

last_id = ""
for order in seeder.generate_orders(5):
    df = pd.DataFrame(order_to_dataframe(order))
    selected_df = df[["order_purchase_timestamp", "order_delivered_customer_date"]]
    table = pa.Table.from_pandas(selected_df)
    tbl.append(table)

# print("stats", seeder._products)
# print("last id must be ", last_id)

data = tbl.scan().to_duckdb("temp")
print(data.sql("select * from temp"))
