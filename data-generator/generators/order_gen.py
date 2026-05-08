import random
from datetime import datetime
from generators.state import State

class OrderGenerator:

    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    channels = ["web", "mobile", "store"]

    def insert(self):
        customer = State.get_random("customers")
        if not customer:
            return None

        order = {
            "customer_id": customer.get("customer_id"),
            "order_date": datetime.utcnow(),
            "status": "pending",
            "channel": random.choice(self.channels),
            "updated_at": datetime.utcnow()
        }

        State.add("orders", order)
        return order

    def update(self):
        order = State.get_random("orders")
        if not order:
            return None

        order["status"] = random.choice(self.statuses)
        order["updated_at"] = datetime.utcnow()
        return order

    def delete(self):
        order = State.get_random("orders")
        if order:
            State.orders.remove(order)
        return order