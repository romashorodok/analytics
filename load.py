import uuid
from seeder import Generator
from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    insert,
    Column,
    Integer,
    Float,
    String,
    DateTime,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from session import DatabaseSessionManager, PostgresDatabaseConfig
import pandas as pd
import asyncio


order_columns = {
    "order_id": "id",
    "customer_id": "customer_id",
    "order_status": "order_status",
    "order_purchase_timestamp": "order_purchase_timestamp",
    "order_approved_at": "order_approved_at",
    "order_delivered_carrier_date": "order_delivered_carrier_date",
    "order_delivered_customer_date": "order_delivered_customer_date",
    "order_estimated_delivery_date": "order_estimated_delivery_date",
}


class Base(DeclarativeBase):
    metadata = MetaData()


class ApiOrder(Base):
    __tablename__ = "api_order"
    id = Column(String, primary_key=True)
    customer_id = Column(String, nullable=False)
    order_status = Column(String, nullable=False)
    order_purchase_timestamp = Column(DateTime, nullable=False)
    order_approved_at = Column(DateTime, nullable=False)
    order_delivered_carrier_date = Column(DateTime, nullable=False)
    order_delivered_customer_date = Column(DateTime, nullable=False)
    order_estimated_delivery_date = Column(DateTime, nullable=False)


class ApiOrderItem(Base):
    __tablename__ = "api_orderitem"
    id = Column(String, nullable=True, primary_key=True)
    order_item_id = Column(String, nullable=False)
    order_id = Column(String, nullable=False)
    product_id = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    shipping_limit_date = Column(DateTime, nullable=False)


# def df_mapper(row) -> pd.DataFrame:
#     return pd.DataFrame([row])


def nat_to_none(timestamp):
    return None if pd.isna(timestamp) else timestamp


def order_mapper(row) -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "order_status": row["order_status"],
                "order_purchase_timestamp": pd.to_datetime(
                    row["order_purchase_timestamp"]
                ),
                "order_delivered_carrier_date": pd.to_datetime(
                    row["order_delivered_carrier_date"]
                ),
                "order_approved_at": pd.to_datetime(row["order_approved_at"]),
                "order_delivered_customer_date": pd.to_datetime(
                    row["order_delivered_customer_date"]
                ),
                "order_estimated_delivery_date": pd.to_datetime(
                    row["order_estimated_delivery_date"]
                ),
            }
        ]
    )
    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        df[col] = (
            df[col]
            .fillna(pd.Timestamp("1970-01-01"))
            .astype("datetime64[us]")
            .apply(nat_to_none)
        )

    df = df.rename(columns=order_columns)

    return df


def order_item_mapper(row) -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "id": str(uuid.uuid4()),
                "order_item_id": str(row["order_item_id"]),
                "order_id": row["order_id"],
                "product_id": row["product_id"],
                "price": row["price"],
                "shipping_limit_date": pd.to_datetime(row["shipping_limit_date"]),
            }
        ]
    )

    for col in [
        "shipping_limit_date",
    ]:
        df[col] = (
            df[col]
            .fillna(pd.Timestamp("1970-01-01"))
            .astype("datetime64[us]")
            .apply(nat_to_none)
        )

    return df


generators: list[tuple[Generator, type[Base]]] = [
    (
        Generator(
            pd.read_csv("dataset/olist_orders_dataset.csv"),
            order_mapper,
        ),
        ApiOrder,
    ),
    (
        Generator(
            pd.read_csv("dataset/olist_order_items_dataset.csv"),
            order_item_mapper,
        ),
        ApiOrderItem,
    ),
]


database_manager = DatabaseSessionManager(PostgresDatabaseConfig().get_uri())


async def main():
    for generator, schema in generators:
        nums, batch_size = generator.full_batch_params(4000)

        for batch in generator.batch(nums, batch_size):
            df = pd.concat(batch, ignore_index=True)

            async with database_manager.session() as conn:
                data = df.to_dict("records")

                items = []
                for row in data:
                    try:
                        item = schema(**row)  # Unpack dictionary
                        items.append(item)
                    except TypeError as e:
                        print(f"Error creating ApiOrder from row: {row}. Error: {e}")

                conn.add_all(items)
                await conn.commit()


if __name__ == "__main__":
    asyncio.run(main())
