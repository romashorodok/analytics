# Generated by Django 5.1.2 on 2024-11-05 00:22

import django.db.models.deletion
import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Geolocation',
            fields=[
                ('id', models.TextField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
            ],
        ),
        migrations.CreateModel(
            name='Order',
            fields=[
                ('id', models.TextField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('order_status', models.TextField()),
                ('customer_id', models.TextField()),
                ('order_purchase_timestamp', models.DateTimeField()),
                ('order_approved_at', models.DateTimeField()),
                ('order_delivered_carrier_date', models.DateTimeField()),
                ('order_delivered_customer_date', models.DateTimeField(null=True)),
                ('order_estimated_delivery_date', models.DateTimeField()),
            ],
        ),
        migrations.CreateModel(
            name='Product',
            fields=[
                ('id', models.TextField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
            ],
        ),
        migrations.CreateModel(
            name='Seller',
            fields=[
                ('id', models.TextField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
            ],
        ),
        migrations.CreateModel(
            name='OrderItem',
            fields=[
                ('id', models.TextField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('shipping_limit_date', models.DateTimeField()),
                ('price', models.IntegerField()),
                ('order_item_id', models.TextField()),
                ('product_id', models.TextField()),
                ('order', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='api.order')),
            ],
        ),
    ]
