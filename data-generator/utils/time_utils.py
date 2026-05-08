from datetime import datetime, timedelta
import random

def get_random_timestamp(days_back=30):
    """
    Generates a random timestamp within the last N days.
    """
    now = datetime.utcnow()
    random_days = random.uniform(0, days_back)
    random_seconds = random.uniform(0, 86400)
    return now - timedelta(days=random_days, seconds=random_seconds)

def format_timestamp(dt):
    """
    Standardizes timestamp formatting for the pipeline.
    """
    return dt.strftime('%Y-%m-%d %H:%M:%S')