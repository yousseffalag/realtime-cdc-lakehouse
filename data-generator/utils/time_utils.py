from datetime import datetime, timedelta
import random

def now():
    return datetime.utcnow()

def random_past_time(days_back=7):
    delta = timedelta(
        seconds=random.randint(0, days_back * 24 * 3600)
    )
    return datetime.utcnow() - delta