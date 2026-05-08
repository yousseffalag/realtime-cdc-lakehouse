import random
from datetime import datetime
from generators.state import State

class ReturnGenerator:

    reasons = ["damaged", "wrong_item", "not_satisfied", "late_delivery"]
    statuses = ["requested", "approved", "rejected", "completed"]

    def insert(self):
        order = State.get_random("orders")
        if not order:
            return None

        data = {
            "order_id": order.get("order_id"),
            "return_date": datetime.utcnow(),
            "reason": random.choice(self.reasons),
            "status": "requested"
        }

        State.add("returns", data)
        return data

    def update(self):
        ret = State.get_random("returns")
        if ret:
            ret["status"] = random.choice(self.statuses)
        return ret

    def delete(self):
        ret = State.get_random("returns")
        if ret:
            State.returns.remove(ret)
        return ret