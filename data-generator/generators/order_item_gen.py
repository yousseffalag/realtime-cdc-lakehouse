import random
from generators.state import State

class OrderItemGenerator:

    def insert(self):
        order = State.get_random("orders")
        product = State.get_random("products")

        if not order or not product:
            return None

        data = {
            "order_id": order.get("order_id"), # Will be None if DB ID not yet assigned
            "product_id": product.get("product_id"),
            "quantity": random.randint(1, 5),
            "unit_price": product["unit_price"]
        }

        State.add("order_items", data)
        return data

    def update(self):
        item = State.get_random("order_items")
        if not item:
            return None

        item["quantity"] = random.randint(1, 10)
        return item

    def delete(self):
        item = State.get_random("order_items")
        if item:
            State.order_items.remove(item)
        return item