import random
from datetime import datetime
from generators.state import State

class CustomerGenerator:

    cities = ["Agadir", "Casablanca", "Rabat", "Marrakech", "Fes"]
    segments = ["regular", "vip", "enterprise"]

    def insert(self):
        data = {
            "full_name": f"Customer_{random.randint(1000,9999)}",
            "city": random.choice(self.cities),
            "segment": random.choice(self.segments),
            "registration_date": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        State.add("customers", data)
        return data

    def update(self):
        customer = State.get_random("customers")
        if not customer:
            return None

        customer["city"] = random.choice(self.cities)
        customer["segment"] = random.choice(self.segments)
        customer["updated_at"] = datetime.utcnow()

        return customer

    def delete(self):
        customer = State.get_random("customers")
        if customer:
            State.customers.remove(customer)
        return customer