from dataclasses import dataclass, field
from typing import Dict, List
from threading import Lock


@dataclass
class GeneratorState:
    """Thread-safe state management for data generator"""
    
    def __init__(self):
        self._customers: List[Dict] = []
        self._products: List[Dict] = []
        self._orders: List[Dict] = []
        self._lock = Lock()
    
    @property
    def customers(self) -> List[Dict]:
        with self._lock:
            return self._customers.copy()
    
    @customers.setter
    def customers(self, value: List[Dict]):
        with self._lock:
            self._customers = value
    
    def add_customers(self, customers: List[Dict]):
        with self._lock:
            self._customers.extend(customers)
    
    def add_products(self, products: List[Dict]):
        with self._lock:
            self._products.extend(products)
    
    def add_orders(self, orders: List[Dict]):
        with self._lock:
            self._orders.extend(orders)
    
    def get_last_customer_id(self) -> int:
        with self._lock:
            return self._customers[-1]['id'] if self._customers else 0
    
    def get_last_product_id(self) -> int:
        with self._lock:
            return self._products[-1]['id'] if self._products else 0
    
    def get_customer_ids(self) -> List[int]:
        with self._lock:
            return [c['id'] for c in self._customers]
    
    def get_product_ids(self) -> List[int]:
        with self._lock:
            return [p['id'] for p in self._products]
    
    def clear(self):
        with self._lock:
            self._customers.clear()
            self._products.clear()
            self._orders.clear()


state = GeneratorState()