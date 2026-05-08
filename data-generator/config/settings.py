import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_CONFIG = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB", "cdc_db"),
        "user": os.getenv("POSTGRES_USER", "cdc_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "cdc_pass"),
    }

    EVENT_RATE_PER_SEC = float(os.getenv("EVENT_RATE", 2))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5))

    CUSTOMER_INSERT_PROB = float(os.getenv("CUSTOMER_INSERT_PROB", 0.05))
    PRODUCT_INSERT_PROB = float(os.getenv("PRODUCT_INSERT_PROB", 0.05))
    ORDER_INSERT_PROB = float(os.getenv("ORDER_INSERT_PROB", 0.30))
    ORDER_ITEM_INSERT_PROB = float(os.getenv("ORDER_ITEM_INSERT_PROB", 0.30))
    ORDER_UPDATE_PROB = float(os.getenv("ORDER_UPDATE_PROB", 0.15))
    PAYMENT_INSERT_PROB = float(os.getenv("PAYMENT_INSERT_PROB", 0.10))
    RETURN_INSERT_PROB = float(os.getenv("RETURN_INSERT_PROB", 0.03))
    ORDER_DELETE_PROB = float(os.getenv("ORDER_DELETE_PROB", 0.02))

    RANDOM_SEED = int(os.getenv("RANDOM_SEED", 42))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()