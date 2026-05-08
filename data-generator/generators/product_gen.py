import random
from datetime import datetime
from generators.state import State

class ProductGenerator:

    catalog = {
        "Electronics": ["iPhone 15", "Samsung S24", "MacBook Pro", "Dell XPS"],
        "Gaming": ["PS5", "Xbox Series X", "Nintendo Switch"],
        "Fashion": ["Nike Air Max", "Adidas Ultraboost", "Levi's Jeans"],
        "Home": ["Dyson Vacuum", "LG Fridge", "Air Conditioner"]
    }

    def insert(self):
        category = random.choice(list(self.catalog.keys()))
        product = random.choice(self.catalog[category])

        data = {
            "category": category,
            "brand": product.split()[0],
            "unit_price": round(random.uniform(50, 2000), 2),
            "stock_qty": random.randint(10, 500),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        State.add("products", data)
        return data

    def update(self):
        product = State.get_random("products")
        if not product:
            return None

        product["unit_price"] = round(random.uniform(50, 2000), 2)
        product["stock_qty"] = random.randint(0, 500)
        product["updated_at"] = datetime.utcnow()

        return product

    def delete(self):
        product = State.get_random("products")
        if product:
            State.products.remove(product)
        return product