from faker import Faker
from typing import Dict, List
import random
from datetime import timedelta
import logging

fake = Faker()
logger = logging.getLogger(__name__)


def generate_orders(customers: List[Dict], count: int = 200) -> List[Dict]:
    """Generate orders - let DB generate order_id"""
    if not customers:
        logger.warning("No customers available")
        return []
    
    orders = []
    statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
    channels = ['web', 'mobile App', 'store', 'B2B']
    
    # Get actual customer_ids from inserted records
    valid_customers = [c for c in customers if c.get('customer_id') is not None]
    
    if not valid_customers:
        logger.error("No valid customers found!")
        return []
    
    for _ in range(count):  # Don't track order_id
        customer = random.choice(valid_customers)
        order_date = fake.date_time_this_year()
        
        orders.append({
            # NO order_id - let SERIAL generate it
            'customer_id': customer['customer_id'],
            'order_date': order_date,
            'status': random.choice(statuses),
            'channel': random.choice(channels),
            'updated_at': order_date + timedelta(days=random.randint(1, 30))
        })
    
    logger.info(f"Generated {len(orders)} orders")
    return orders