import uuid
from django.db import models


class Geolocation(models.Model):
    id = models.TextField(primary_key=True, default=uuid.uuid4, editable=False)

    pass


class Seller(models.Model):
    id = models.TextField(primary_key=True, default=uuid.uuid4, editable=False)

    pass


class Product(models.Model):
    id = models.TextField(primary_key=True, default=uuid.uuid4, editable=False)

    pass


class Order(models.Model):
    id = models.TextField(primary_key=True, default=uuid.uuid4, editable=False)

    order_status = models.TextField()
    customer_id = models.TextField()
    order_purchase_timestamp = models.DateTimeField()
    order_approved_at = models.DateTimeField()
    order_delivered_carrier_date = models.DateTimeField()
    order_delivered_customer_date = models.DateTimeField(null=True)
    order_estimated_delivery_date = models.DateTimeField(null=True)


class OrderItem(models.Model):
    id = models.TextField(primary_key=True, default=uuid.uuid4, editable=False)

    shipping_limit_date = models.DateTimeField()
    price = models.IntegerField()
    order = models.ForeignKey(Order, on_delete=models.CASCADE)

    # order_id = models.TextField()
    order_item_id = models.TextField()
    product_id = models.TextField()
