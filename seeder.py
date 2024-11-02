import pandas as pd
import random
import uuid
from dataclasses import dataclass, asdict
from typing import List


@dataclass
class PaymentDetail:
    order_id: str
    payment_sequential: int
    payment_type: str
    payment_installments: int
    payment_value: float


@dataclass
class Product:
    product_id: str
    product_category_name: str
    product_name_length: int
    product_description_length: int
    product_photos_qty: int
    product_weight_g: int
    product_length_cm: int
    product_height_cm: int
    product_width_cm: int


@dataclass
class Geolocation:
    geolocation_zip_code_prefix: int
    geolocation_lat: float
    geolocation_lng: float
    geolocation_city: str
    geolocation_state: str


@dataclass
class Seller:
    seller_id: str
    seller_zip_code_prefix: int
    seller_city: str
    seller_state: str


@dataclass
class OrderItem:
    order_id: str
    order_item_id: str
    product_id: str
    seller_id: str
    shipping_limit_date: pd.Timestamp
    price: float
    freight_value: float


@dataclass
class Review:
    review_id: str
    order_id: str
    review_score: int
    review_comment_title: str
    review_comment_message: str
    review_creation_date: pd.Timestamp
    review_answer_timestamp: pd.Timestamp


@dataclass
class Order:
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: pd.Timestamp
    order_approved_at: pd.Timestamp
    order_delivered_carrier_date: pd.Timestamp
    order_delivered_customer_date: pd.Timestamp
    order_estimated_delivery_date: pd.Timestamp
    items: List[OrderItem]
    payment_details: List[PaymentDetail]
    review: Review


class EcommerceSeeder:
    def __init__(self):
        self._customers = pd.read_csv("dataset/olist_customers_dataset.csv")
        self._geolocation = pd.read_csv("dataset/olist_geolocation_dataset.csv")
        self._order_items = pd.read_csv("dataset/olist_order_items_dataset.csv")
        self._order_payments = pd.read_csv("dataset/olist_order_payments_dataset.csv")
        self._order_reviews = pd.read_csv("dataset/olist_order_reviews_dataset.csv")
        self._orders = pd.read_csv("dataset/olist_orders_dataset.csv")
        self._products = pd.read_csv("dataset/olist_products_dataset.csv")
        self._sellers = pd.read_csv("dataset/olist_sellers_dataset.csv")
        self._products_translate = pd.read_csv(
            "dataset/product_category_name_translation.csv"
        )

        self._product_index = 0
        self._payment_index = 0
        self._order_item_index = 0
        self._review_index = 0
        self._order_index = 0

    def get_product(self) -> Product:
        row = self._products.iloc[self._product_index]
        self._product_index = (self._product_index + 1) % len(self._products)
        return Product(
            product_id=row["product_id"],
            product_category_name=row["product_category_name"],
            product_name_length=row["product_name_length"],
            product_description_length=row["product_description_length"],
            product_photos_qty=row["product_photos_qty"],
            product_weight_g=row["product_weight_g"],
            product_length_cm=row["product_length_cm"],
            product_height_cm=row["product_height_cm"],
            product_width_cm=row["product_width_cm"],
        )

    def get_payment_detail(self, order_id: str) -> PaymentDetail:
        row = self._order_payments.iloc[self._payment_index]
        self._payment_index = (self._payment_index + 1) % len(self._order_payments)
        return PaymentDetail(
            order_id=order_id,
            payment_sequential=row.get("payment_sequential", 1),
            payment_type=row["payment_type"],
            payment_installments=row["payment_installments"],
            payment_value=row["payment_value"],
        )

    def get_order_item(self, order_id: str, row) -> OrderItem:
        # row = self._order_items.iloc[self._order_item_index]
        # self._order_item_index = (self._order_item_index + 1) % len(self._order_items)
        return OrderItem(
            order_id=order_id,
            order_item_id=row["order_item_id"],
            product_id=row["product_id"],
            seller_id=row["seller_id"],
            shipping_limit_date=pd.to_datetime(row["shipping_limit_date"]),
            price=float(row["price"]),
            freight_value=float(row["freight_value"]),
        )

    def get_review(self, order_id: str) -> Review:
        row = self._order_reviews.iloc[self._review_index]
        self._review_index = (self._review_index + 1) % len(self._order_reviews)
        return Review(
            review_id=str(uuid.uuid4()),
            order_id=order_id,
            review_score=row.get("review_score", 3),
            review_comment_title=row.get("review_comment_title", ""),
            review_comment_message=row.get("review_comment_message", ""),
            review_creation_date=pd.to_datetime(row["review_creation_date"]),
            review_answer_timestamp=pd.to_datetime(row["review_answer_timestamp"]),
        )

    def get_order(self) -> Order:
        row = self._orders.iloc[self._order_index]
        self._order_index = (self._order_index + 1) % len(self._orders)
        # order_id = str(uuid.uuid4())
        order_id = row["order_id"]
        customer_id = row["customer_id"]

        items = [
            self.get_order_item(order_id, item_row)
            for _, item_row in self._order_items[
                self._order_items["order_id"] == order_id
            ].iterrows()
        ]

        payment_details = [self.get_payment_detail(order_id) for _ in range(1)]
        review = self.get_review(order_id)

        return Order(
            order_id=order_id,
            customer_id=customer_id,
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
            items=items,
            payment_details=payment_details,
            review=review,
        )

    def generate_orders(self, num_orders: int):
        for _ in range(num_orders):
            yield self.get_order()

    def get_random_product(self) -> Product:
        row = self._products.sample(1).iloc[0]
        return Product(
            product_id=row["product_id"],
            product_category_name=row["product_category_name"],
            product_name_length=row["product_name_length"],
            product_description_length=row["product_description_length"],
            product_photos_qty=row["product_photos_qty"],
            product_weight_g=row["product_weight_g"],
            product_length_cm=row["product_length_cm"],
            product_height_cm=row["product_height_cm"],
            product_width_cm=row["product_width_cm"],
        )

    def get_random_payment_detail(self, order_id: str) -> PaymentDetail:
        row = self._order_payments.sample(1).iloc[0]
        return PaymentDetail(
            order_id=order_id,
            payment_sequential=random.randint(1, 5),
            payment_type=row["payment_type"],
            payment_installments=row["payment_installments"],
            payment_value=row["payment_value"],
        )

    def get_random_order_item(self, order_id: str) -> OrderItem:
        row = self._order_items.sample(1).iloc[0]
        return OrderItem(
            order_id=order_id,
            order_item_id=str(uuid.uuid4()),
            product_id=row["product_id"],
            seller_id=row["seller_id"],
            shipping_limit_date=pd.to_datetime(row["shipping_limit_date"]),
            price=float(row["price"]),
            freight_value=float(row["freight_value"]),
        )

    def get_random_review(self, order_id: str) -> Review:
        row = self._order_reviews.sample(1).iloc[0]
        return Review(
            review_id=str(uuid.uuid4()),
            order_id=order_id,
            review_score=random.randint(1, 5),
            review_comment_title=row.get("review_comment_title", ""),
            review_comment_message=row.get("review_comment_message", ""),
            review_creation_date=pd.to_datetime(row["review_creation_date"]),
            review_answer_timestamp=pd.to_datetime(row["review_answer_timestamp"]),
        )

    def get_random_order(self) -> Order:
        row = self._orders.sample(1).iloc[0]
        order_id = str(uuid.uuid4())
        customer_id = row["customer_id"]

        items = [
            self.get_random_order_item(order_id) for _ in range(random.randint(1, 3))
        ]
        payment_details = [
            self.get_random_payment_detail(order_id)
            for _ in range(random.randint(1, 2))
        ]
        review = self.get_random_review(order_id)

        return Order(
            order_id=order_id,
            customer_id=customer_id,
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
            items=items,
            payment_details=payment_details,
            review=review,
        )

    def generate_random_orders(self, num_orders: int):
        for _ in range(num_orders):
            yield self.get_random_order()


def order_to_dataframe(order: Order) -> pd.DataFrame:
    order_data = asdict(order)

    # Prepare a list to hold rows of the DataFrame
    rows = [order_data]

    # Iterate through each item in the order
    # for item in order.items:
    #     # Extract item details
    #     item_data = {
    #         "freight_value": item.freight_value,
    #         "order_id": item.order_id,
    #         "order_item_id": item.order_item_id,
    #         "price": item.price,
    #         "product_id": item.product_id,
    #         "seller_id": item.seller_id,
    #         "shipping_limit_date": item.shipping_limit_date,
    #     }
    #
    #     # Iterate through each payment detail
    #     for payment in order.payment_details:
    #         # Create a new combined entry for the DataFrame
    #         combined_data = {
    #             **order_data,
    #             **item_data,
    #             "payment_order_id": payment.order_id,
    #             "payment_installments": payment.payment_installments,
    #             "payment_sequential": payment.payment_sequential,
    #             "payment_type": payment.payment_type,
    #             "payment_value": payment.payment_value,
    #         }
    #         # Append the combined data to the rows list
    #         rows.append(combined_data)

    # Create a DataFrame from the collected rows
    df = pd.DataFrame(rows)

    # Convert timestamp columns to milliseconds
    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "shipping_limit_date",
        "review_creation_date",
        "review_answer_timestamp",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("datetime64[us]")  # Downcast to milliseconds

    return df
