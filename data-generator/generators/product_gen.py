from faker import Faker
from typing import Dict, List
import random

fake = Faker()


def generate_products(count: int = 100) -> List[Dict]:
    """Generate product data - let DB generate product_id"""
    products = []
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Toys', 'Beauty']
    brands = ['Nike', 'Apple', 'Sony', 'Samsung', 'Adidas', 'LG', 'HP', 'Dell']
    
    for _ in range(count):  # Don't track ID count
        products.append({
            # NO product_id - let SERIAL generate it
            'category': random.choice(categories),
            'brand': random.choice(brands),
            'unit_price': round(random.uniform(10, 1000), 2),
            'stock_qty': random.randint(0, 500),
            'created_at': fake.date_time_this_year(),
            'updated_at': fake.date_time_this_year()
        })
    
    return products