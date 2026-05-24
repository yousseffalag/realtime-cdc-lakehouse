from typing import Dict, List
import random
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


def generate_returns(orders: List[Dict]) -> List[Dict]:
    """Generate returns - NO ID (let DB auto-generate)"""
    if not orders:
        return []
    
    returns = []
    reasons = ['defective', 'customer regret', 'wrong item', 'damaged']
    statuses = ['requested', 'approved', 'rejected', 'completed']
    
    # About 10% of orders have returns
    eligible_orders = [o for o in orders if o.get('status') in ['delivered', 'completed']]
    
    if eligible_orders:
        num_returns = min(len(eligible_orders) // 10, len(eligible_orders))
        return_orders = random.sample(eligible_orders, num_returns) if num_returns > 0 else []
        
        for order in return_orders:
            returns.append({
                # NO return_id - let SERIAL generate it
                'order_id': order['order_id'],
                'return_date': order['order_date'] + timedelta(days=random.randint(1, 15)),
                'reason': random.choice(reasons),
                'status': random.choice(statuses)
            })
    
    logger.info(f"Generated {len(returns)} returns")
    return returns