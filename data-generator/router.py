import random
from config.settings import settings

from generators.customer_gen import CustomerGenerator
from generators.product_gen import ProductGenerator
from generators.order_gen import OrderGenerator
from generators.order_item_gen import OrderItemGenerator
from generators.payment_gen import PaymentGenerator
from generators.return_gen import ReturnGenerator


class ActivityRouter:

    def __init__(self):
        # =====================================================
        # GENERATORS
        # =====================================================
        self.customers = CustomerGenerator()
        self.products = ProductGenerator()
        self.orders = OrderGenerator()
        self.order_items = OrderItemGenerator()
        self.payments = PaymentGenerator()
        self.returns = ReturnGenerator()

        # probability helper (clean readability)
        self.p = settings

    # =========================================================
    # MAIN DECISION ENGINE
    # =========================================================

    def route(self):
        r = random.random()
        
        # Cumulative probabilities for clean routing logic
        p_customer = self.p.CUSTOMER_INSERT_PROB
        p_product = p_customer + self.p.PRODUCT_INSERT_PROB
        p_order = p_product + self.p.ORDER_INSERT_PROB
        p_order_item = p_order + self.p.ORDER_ITEM_INSERT_PROB
        p_order_update = p_order_item + self.p.ORDER_UPDATE_PROB
        p_payment = p_order_update + self.p.PAYMENT_INSERT_PROB
        p_return = p_payment + self.p.RETURN_INSERT_PROB

        if r < p_customer:
            return self._customer_insert()
        elif r < p_product:
            return self._product_insert()
        elif r < p_order:
            return self._order_insert()
        elif r < p_order_item:
            return self._order_item_insert()
        elif r < p_order_update:
            return self._order_update()
        elif r < p_payment:
            return self._payment_insert()
        elif r < p_return:
            return self._return_insert()
        else:
            return self._order_delete()

    # =========================================================
    # ACTION HANDLERS
    # =========================================================

    def _customer_insert(self):
        data = self.customers.insert()
        return ("customers", "INSERT", data)

    def _product_insert(self):
        data = self.products.insert()
        return ("products", "INSERT", data)

    def _order_insert(self):
        data = self.orders.insert()
        return ("orders", "INSERT", data)

    def _order_item_insert(self):
        data = self.order_items.insert()
        return ("order_items", "INSERT", data)

    def _order_update(self):
        data = self.orders.update()
        return ("orders", "UPDATE", data)

    def _payment_insert(self):
        data = self.payments.insert()
        return ("payments", "INSERT", data)

    def _return_insert(self):
        data = self.returns.insert()
        return ("returns", "INSERT", data)

    def _order_delete(self):
        data = self.orders.delete()
        return ("orders", "DELETE", data)