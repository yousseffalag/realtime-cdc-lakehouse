import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data before insertion to prevent NULL foreign keys"""
    
    @staticmethod
    def validate_customers(customers: List[Dict]) -> List[Dict]:
        """Ensure customers have valid IDs"""
        valid = []
        for cust in customers:
            if cust.get('id') is not None:
                valid.append(cust)
            else:
                logger.warning(f"Skipping customer with no ID: {cust}")
        return valid
    
    @staticmethod
    def validate_orders(orders: List[Dict], existing_customer_ids: set) -> List[Dict]:
        """Ensure orders reference existing customers"""
        valid = []
        for order in orders:
            customer_id = order.get('customer_id')
            if customer_id is not None and customer_id in existing_customer_ids:
                valid.append(order)
            else:
                logger.warning(f"Skipping order with invalid customer_id: {customer_id}")
        return valid
    
    @staticmethod
    def validate_order_items(items: List[Dict], existing_order_ids: set, existing_product_ids: set) -> List[Dict]:
        """Ensure order items reference existing orders and products"""
        valid = []
        for item in items:
            order_id = item.get('order_id')
            product_id = item.get('product_id')
            
            if order_id is not None and order_id in existing_order_ids:
                if product_id is not None and product_id in existing_product_ids:
                    valid.append(item)
                else:
                    logger.warning(f"Skipping item with invalid product_id: {product_id}")
            else:
                logger.warning(f"Skipping item with invalid order_id: {order_id}")
        return valid