import random
from datetime import datetime
from generators.state import State

class PaymentGenerator:

    methods = ["card", "paypal", "cash", "bank_transfer"]

    def insert(self):
        order = State.get_random("orders")
        if not order:
            return None

        data = {
            "order_id": order.get("order_id"),
            "payment_date": datetime.utcnow(),
            "payment_method": random.choice(self.methods),
            "amount": round(random.uniform(50, 3000), 2),
            "payment_status": "completed"
        }

        State.add("payments", data)
        return data

    def update(self):
        payment = State.get_random("payments")
        if payment:
            payment["payment_status"] = "refunded"
        return payment

    def delete(self):
        payment = State.get_random("payments")
        if payment:
            State.payments.remove(payment)
        return payment