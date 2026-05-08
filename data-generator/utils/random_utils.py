import random
import string

def generate_sku(length=8):
    """
    Generates a random alphanumeric SKU.
    """
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def weighted_choice(choices, weights):
    """
    Returns a random choice based on weights.
    """
    return random.choices(choices, weights=weights, k=1)[0]

def random_phone():
    """
    Generates a simple random phone number.
    """
    return f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
