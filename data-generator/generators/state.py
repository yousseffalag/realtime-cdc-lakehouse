class State:
    customers = []
    products = []
    orders = []
    order_items = []
    payments = []
    returns = []

    @classmethod
    def add(cls, table, value):
        getattr(cls, table).append(value)

    @classmethod
    def get_random(cls, table):
        import random
        data = getattr(cls, table)
        return random.choice(data) if data else None