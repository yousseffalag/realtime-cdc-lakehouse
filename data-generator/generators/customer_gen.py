from faker import Faker
from typing import Dict, List
import random

fake = Faker()


def generate_customers(count: int = 50) -> List[Dict]:
    """Generate customer data - let DB generate customer_id"""
    customers = []
    segments = ['regular', 'vip', 'enterprise']
    
    for _ in range(count):  # Don't track ID count
        customers.append({
            # NO customer_id - let SERIAL generate it
            'full_name': fake.name(),
            'city': fake.city(),
            'segment': random.choice(segments),
            'registration_date': fake.date_time_this_year(),
            'updated_at': fake.date_time_this_year()
        })
    
    return customers