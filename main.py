from dataclasses import asdict
from typing import Any
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table import Table, TableProperties
from pyiceberg.transforms import DayTransform, YearTransform, IdentityTransform
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    TimestampType,
    DoubleType,
    StringType,
    NestedField,
)

from seeder import (
    Generator,
    Geolocation,
    Order,
    OrderItem,
    Product,
    Review,
    Seller,
)
import pandas as pd

pg_conn = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"

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

order_schema = Schema(
    # Start partition
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
    NestedField(
        field_id=3,
        name="order_status",
        field_type=StringType(),
        required=False,
    ),
    # End parition
    NestedField(
        field_id=4,
        name="order_approved_at",
        field_type=TimestampType(),
        required=False,
    ),
    NestedField(
        field_id=5,
        name="order_delivered_carrier_date",
        field_type=TimestampType(),
        required=False,
    ),
    NestedField(
        field_id=6,
        name="order_estimated_delivery_date",
        field_type=TimestampType(),
        required=False,
    ),
    NestedField(
        field_id=7,
        name="order_id",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=8,
        name="customer_id",
        field_type=StringType(),
        required=False,
    ),
)

order_partition_spec = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1,
        transform=YearTransform(),
        name="order_purchase_timestamp",
    ),
    PartitionField(
        source_id=2,
        field_id=2,
        transform=DayTransform(),
        name="order_delivered_customer_date",
    ),
    PartitionField(
        source_id=3,
        field_id=3,
        transform=IdentityTransform(),
        name="order_status",
    ),
)

order_item_schema = Schema(
    # Start parition
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
    # End parition
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

order_item_select = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

order_item_parittion = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1,
        transform=IdentityTransform(),
        name="product_id",
    ),
    PartitionField(
        source_id=2,
        field_id=2,
        transform=DayTransform(),
        name="shipping_limit_date",
    ),
)

product_schema = Schema(
    # Start parittion
    NestedField(
        field_id=1,
        name="product_category_name",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=2,
        name="product_id",
        field_type=StringType(),
        required=False,
    ),
    # End parittion
    NestedField(
        field_id=3,
        name="product_name_lenght",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=4,
        name="product_description_lenght",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=5,
        name="product_photos_qty",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=6,
        name="product_weight_g",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=7,
        name="product_length_cm",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=8,
        name="product_height_cm",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=9,
        name="product_width_cm",
        field_type=DoubleType(),
        required=False,
    ),
)

product_partition = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1,
        transform=IdentityTransform(),
        name="product_category_name",
    ),
)

seller_schema = Schema(
    NestedField(
        field_id=1,
        name="seller_state",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=2,
        name="seller_id",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=3,
        name="seller_zip_code_prefix",
        field_type=LongType(),
        required=False,
    ),
    NestedField(
        field_id=4,
        name="seller_city",
        field_type=StringType(),
        required=False,
    ),
)

seller_partition = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1,
        transform=IdentityTransform(),
        name="seller_state",
    ),
)

geolocation_schema = Schema(
    # Start parittion
    NestedField(
        field_id=1,
        name="geolocation_state",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=2,
        name="geolocation_city",
        field_type=StringType(),
        required=False,
    ),
    # End parittion
    NestedField(
        field_id=3,
        name="geolocation_zip_code_prefix",
        field_type=LongType(),
        required=False,
    ),
    NestedField(
        field_id=4,
        name="geolocation_lat",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=5,
        name="geolocation_lng",
        field_type=DoubleType(),
        required=False,
    ),
)

geolocation_partition = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1,
        transform=IdentityTransform(),
        name="geolocation_state",
    ),
    PartitionField(
        source_id=2,
        field_id=2,
        transform=IdentityTransform(),
        name="geolocation_city",
    ),
)

review_schema = Schema(
    # Start partition
    NestedField(
        field_id=1,
        name="review_creation_date",
        field_type=TimestampType(),
        required=False,
    ),
    NestedField(
        field_id=2,
        name="review_score",
        field_type=LongType(),
        required=False,
    ),
    # End partition
    NestedField(
        field_id=3,
        name="review_id",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=4,
        name="order_id",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=5,
        name="review_comment_title",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=6,
        name="review_comment_message",
        field_type=StringType(),
        required=False,
    ),
    NestedField(
        field_id=7,
        name="review_answer_timestamp",
        field_type=TimestampType(),
        required=False,
    ),
)

review_partition = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1,
        transform=YearTransform(),
        name="review_creation_date",
    ),
    PartitionField(
        source_id=2,
        field_id=2,
        transform=IdentityTransform(),
        name="review_score",
    ),
)

# https://github.com/apache/iceberg-python/blob/9fd440edba6d6f45821fea244fa00e2ed1f0f363/pyiceberg/table/__init__.py#L179C63-L179C71
properties: dict[str, Any] = {
    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: 512 * 1024 * 1024
}

identifier = "ecommerce"

product_identifier = f"{identifier}.products"
seller_identifier = f"{identifier}.sellers"
geolocation_identitfier = f"{identifier}.geolocations"
review_identitfier = f"{identifier}.reviews"
order_item_identifier = f"{identifier}.order_items"
order_identifier = f"{identifier}.orders"

catalog.create_namespace_if_not_exists(identifier)


def reviews_mapper(row) -> pd.DataFrame:
    row = Review(
        review_id=row.get("review_id"),
        order_id=row.get("order_id"),
        review_score=row.get("review_score", 3),
        review_comment_title=str(row.get("review_comment_title", "")),
        review_comment_message=str(row.get("review_comment_message", "")),
        review_creation_date=pd.to_datetime(row["review_creation_date"]),
        review_answer_timestamp=pd.to_datetime(row["review_answer_timestamp"]),
    )

    df = pd.DataFrame([asdict(row)])
    for col in ["review_creation_date", "review_answer_timestamp"]:
        df[col] = df[col].astype("datetime64[us]")

    return df


reviews_generator = Generator(
    pd.read_csv("dataset/olist_order_reviews_dataset.csv"),
    reviews_mapper,
)


def order_mapper(row) -> pd.DataFrame:
    row = Order(
        order_id=row["order_id"],
        customer_id=row["customer_id"],
        order_status=row["order_status"],
        order_purchase_timestamp=pd.to_datetime(row["order_purchase_timestamp"]),
        order_approved_at=pd.to_datetime(row["order_approved_at"]),
        order_delivered_carrier_date=pd.to_datetime(
            row["order_delivered_carrier_date"]
        ),
        order_delivered_customer_date=pd.to_datetime(
            row["order_delivered_customer_date"]
        ),
        order_estimated_delivery_date=pd.to_datetime(
            row["order_estimated_delivery_date"]
        ),
    )

    df = pd.DataFrame([asdict(row)])
    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        df[col] = df[col].astype("datetime64[us]")

    return df


order_generator = Generator(
    pd.read_csv("dataset/olist_orders_dataset.csv"),
    order_mapper,
)


def order_item_mapper(row) -> pd.DataFrame:
    row = OrderItem(
        order_id=row["order_id"],
        order_item_id=row["order_item_id"],
        product_id=row["product_id"],
        seller_id=row["seller_id"],
        shipping_limit_date=pd.to_datetime(row["shipping_limit_date"]),
        price=float(row["price"]),
        freight_value=float(row["freight_value"]),
    )

    df = pd.DataFrame([asdict(row)])
    df["shipping_limit_date"] = df["shipping_limit_date"].astype("datetime64[us]")

    return df


order_items_generator = Generator(
    pd.read_csv("dataset/olist_order_items_dataset.csv"),
    order_item_mapper,
)


def geolocation_mapper(row) -> pd.DataFrame:
    row = Geolocation(
        geolocation_zip_code_prefix=int(row["geolocation_zip_code_prefix"]),
        geolocation_lat=row["geolocation_lat"],
        geolocation_lng=row["geolocation_lng"],
        geolocation_city=row["geolocation_city"],
        geolocation_state=row["geolocation_state"],
    )

    return pd.DataFrame([asdict(row)])


geolocation_generator = Generator(
    pd.read_csv("dataset/olist_geolocation_dataset.csv"),
    geolocation_mapper,
)


def seller_mapper(row) -> pd.DataFrame:
    row = Seller(
        seller_id=row["seller_id"],
        seller_zip_code_prefix=int(row["seller_zip_code_prefix"]),
        seller_city=row["seller_city"],
        seller_state=row["seller_state"],
    )

    return pd.DataFrame([asdict(row)])


seller_generator = Generator(
    pd.read_csv("dataset/olist_sellers_dataset.csv"),
    seller_mapper,
)


def product_mapper(row) -> pd.DataFrame:
    row = Product(
        product_id=row["product_id"],
        product_category_name=row["product_category_name"],
        product_name_lenght=row["product_name_lenght"],
        product_description_lenght=row["product_description_lenght"],
        product_photos_qty=row["product_photos_qty"],
        product_weight_g=row["product_weight_g"],
        product_length_cm=row["product_length_cm"],
        product_height_cm=row["product_height_cm"],
        product_width_cm=row["product_width_cm"],
    )

    return pd.DataFrame([asdict(row)])


product_generator = Generator(
    pd.read_csv("dataset/olist_products_dataset.csv"),
    product_mapper,
)

# for id in [
#     review_identitfier,
#     geolocation_identitfier,
#     seller_identifier,
#     product_identifier,
#     order_item_identifier,
#     order_identifier,
# ]:
#     try:
#         catalog.drop_table(id)
#     except NoSuchTableError:
#         pass

review_tbl = catalog.create_table_if_not_exists(
    review_identitfier,
    schema=review_schema,
    properties=properties,
    partition_spec=review_partition,
)

geolocation_tbl = catalog.create_table_if_not_exists(
    geolocation_identitfier,
    schema=geolocation_schema,
    properties=properties,
    partition_spec=geolocation_partition,
)

seller_tbl = catalog.create_table_if_not_exists(
    seller_identifier,
    schema=seller_schema,
    properties=properties,
    partition_spec=seller_partition,
)

product_tbl = catalog.create_table_if_not_exists(
    product_identifier,
    schema=product_schema,
    properties=properties,
    partition_spec=product_partition,
)

order_item_tbl = catalog.create_table_if_not_exists(
    order_item_identifier,
    schema=order_item_schema,
    properties=properties,
    partition_spec=order_item_parittion,
)

order_tbl = catalog.create_table_if_not_exists(
    order_identifier,
    schema=order_schema,
    properties=properties,
    partition_spec=order_partition_spec,
)

tbls: list[tuple[Table, Generator]] = [
    (review_tbl, reviews_generator),
    (geolocation_tbl, geolocation_generator),
    (seller_tbl, seller_generator),
    (product_tbl, product_generator),
    (order_item_tbl, order_items_generator),
    (order_tbl, order_generator),
]

# for tbl, generator in tbls:
#     # duckdb = tbl.scan().to_duckdb("temp")
#     # print(duckdb.sql("select * from temp"))
#
#     num, batch_size = generator.full_batch_params(2000)
#     for data in generator.batch(num, batch_size):
#         df = pd.concat(data, ignore_index=True)
#         print(df)
#         pat = pa.Table.from_pandas(df)
#         tbl.append(pat)
