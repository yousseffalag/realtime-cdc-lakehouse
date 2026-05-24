from typing import Dict, List
import random
import logging

logger = logging.getLogger(__name__)


def generate_order_items(orders: List[Dict], products: List[Dict]) -> List[Dict]:
    """Generate order items - let DB generate order_item_id"""
    if not orders or not products:
        return []
    
    order_items = []
    
    valid_orders = [o for o in orders if o.get('order_id') is not None]
    valid_products = [p for p in products if p.get('product_id') is not None]
    
    if not valid_orders or not valid_products:
        return []
    
    for order in valid_orders:
        num_items = random.randint(1, 5)
        
        for _ in range(num_items):
            product = random.choice(valid_products)
            quantity = random.randint(1, 10)
            
            order_items.append({
                # NO order_item_id - let SERIAL generate it
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': product.get('unit_price', 0)
                # total_amount is GENERATED ALWAYS - don't include
            })
    
    logger.info(f"Generated {len(order_items)} order items")
    return order_items