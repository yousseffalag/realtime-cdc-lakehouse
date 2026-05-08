import time
import logging
import random
from config.settings import settings
from router import ActivityRouter
from db import DBSession
from sqlalchemy import text

# =========================================================
# LOGGING SETUP
# =========================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

router = ActivityRouter()

# =========================================================
# DB EXECUTION HELPERS
# =========================================================

def execute_operation(session, table, operation, data):
    """
    Executes a SQL operation and returns the generated ID if it's an INSERT.
    """
    if not data:
        return None

    try:
        if operation == "INSERT":
            columns = ", ".join(data.keys())
            placeholders = ", ".join([f":{k}" for k in data.keys()])
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING *"
            result = session.execute(text(sql), data)
            row = result.fetchone()
            if row:
                return dict(row._mapping)
        
        elif operation == "UPDATE":
            # Assuming 'id' is in the data for update targeting
            pk_col = f"{table[:-1]}_id" if not table.endswith('s') else f"{table[:-1]}_id"
            # Special cases for pluralization
            if table == "order_items": pk_col = "item_id"
            
            update_id = data.get(pk_col)
            if not update_id: return None
            
            set_clause = ", ".join([f"{k} = :{k}" for k in data.keys() if k != pk_col])
            sql = f"UPDATE {table} SET {set_clause} WHERE {pk_col} = :{pk_col}"
            session.execute(text(sql), data)
            return data

        elif operation == "DELETE":
            pk_col = f"{table[:-1]}_id"
            if table == "order_items": pk_col = "item_id"
            
            delete_id = data.get(pk_col)
            if not delete_id: return None
            
            sql = f"DELETE FROM {table} WHERE {pk_col} = :id"
            session.execute(text(sql), {"id": delete_id})
            return data

    except Exception as e:
        logger.error(f"DB Error on {table} {operation}: {e}")
        return None

# =========================================================
# MAIN LOOP
# =========================================================

def run():
    logger.info("Starting CDC Data Generator...")

    while True:
        with DBSession() as session:
            # Route an activity
            table, operation, data = router.route()
            
            # Execute in DB
            result_data = execute_operation(session, table, operation, data)
            
            if result_data:
                logger.info(f"Stream: {operation} on {table}")
            
            # Sleep to simulate real-time
            time.sleep(random.uniform(0.5, 3.0))

if __name__ == "__main__":
    run()
