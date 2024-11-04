from typing import Any, Callable

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table

import pyarrow as pa

pg_conn = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"


class Dimension:
    def __init__(
        self, identifier: str, schema: Schema, partition: PartitionSpec | None = None
    ):
        self.identifier = identifier
        self.schema = schema
        self.partition = partition
        self.fields = set(map(lambda field: field.name, schema.fields))


class JoinOnDimension:
    def __init__(self, on_dim_name: str, cb: Callable[[pa.RecordBatch, Table], Any]):
        self.on_dim_name = on_dim_name
        self.cb = cb


class Cube:
    def __init__(self, dim: dict[str, Dimension]):
        self.catalog = SqlCatalog(
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
        self.dim = dim

    def join(self, root_dim_name: str, join: JoinOnDimension):
        root_dim = self.dim.get(root_dim_name)
        if not root_dim:
            raise ValueError("Not found root dimenion table name")

        join_dim = self.dim.get(join.on_dim_name)
        if not join_dim:
            raise ValueError("Not found join dimenion table name")

        root_tbl = self.catalog.load_table(root_dim.identifier)
        join_tbl = self.catalog.load_table(join_dim.identifier)

        reader = root_tbl.scan().to_arrow_batch_reader()

        for batch in reader:
            join.cb(batch, join_tbl)
            # record: pa.RecordBatch = batch
            # print(type(batch))

        reader.close()
