# Generated by Django 5.1.2 on 2024-11-05 01:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='order',
            name='order_estimated_delivery_date',
            field=models.DateTimeField(null=True),
        ),
    ]
