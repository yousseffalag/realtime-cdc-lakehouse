from typing import Dict, List
import random
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


def generate_payments(orders: List[Dict]) -> List[Dict]:
    """Generate payments - NO ID (let DB auto-generate)"""
    if not orders:
        return []
    
    payments = []
    payment_methods = ['card', 'paypal', 'cash', 'bank_transfer']
    statuses = ['completed', 'pending', 'failed']
    
    # Only generate payments for completed/delivered orders
    eligible_orders = [o for o in orders if o.get('status') in ['completed', 'delivered', 'confirmed', 'shipped']]
    
    for order in eligible_orders:
        payments.append({
            # NO payment_id - let SERIAL generate it
            'order_id': order['order_id'],
            'payment_date': order['order_date'] + timedelta(days=random.randint(0, 3)),
            'payment_method': random.choice(payment_methods),
            'amount': round(random.uniform(50, 2000), 2),
            'payment_status': random.choice(statuses)
        })
    
    logger.info(f"Generated {len(payments)} payments")
    return payments