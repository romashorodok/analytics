from celery import shared_task
from data_store.order_item import OrderItem


@shared_task
def order_items(order_id: str = "73fc7af87114b39712e6da79b0a377eb"):
    cube = OrderItem.new_cube()
    # cube.join(OrderItem.identifier, order_id)

    # print("get cube", cube)
    # pass
