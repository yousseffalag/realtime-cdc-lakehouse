from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, connection_string: str):
        """Initialize database connection"""
        self.engine = create_engine(
            connection_string,
            echo=False,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20
        )
        self.Session = sessionmaker(bind=self.engine)
        
        # Define ID columns for each table
        self.id_columns = {
            'customers': 'customer_id',
            'products': 'product_id',
            'orders': 'order_id',
            'order_items': 'order_item_id',
            'payments': 'payment_id',
            'returns': 'return_id'
        }
        
        logger.info("Database manager initialized")
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            session = self.Session()
            session.execute(text("SELECT 1"))
            session.close()
            logger.info("✓ Database connection successful")
            return True
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            raise
    
    def insert_data(self, table_name: str, data: List[Dict]) -> List[int]:
        """
        Insert data and return generated IDs
        IMPORTANT: Do NOT include ID field in data - let DB auto-generate
        """
        if not data:
            logger.warning(f"No data to insert into {table_name}")
            return []
        
        session = self.Session()
        generated_ids = []
        
        try:
            id_column = self.id_columns.get(table_name, f"{table_name[:-1]}_id")
            
            for record in data:
                # Remove None values and ensure NO ID field is present
                record = {k: v for k, v in record.items() if v is not None}
                
                # Explicitly remove ID field if present (safety check)
                record.pop(id_column, None)
                
                if not record:
                    continue
                
                columns = ', '.join(record.keys())
                placeholders = ', '.join([f":{k}" for k in record.keys()])
                
                sql = text(f"""
                    INSERT INTO {table_name} ({columns}) 
                    VALUES ({placeholders})
                    RETURNING {id_column}
                """)
                
                result = session.execute(sql, record)
                generated_id = result.scalar()
                generated_ids.append(generated_id)
            
            session.commit()
            logger.info(f"✓ Inserted {len(data)} records into {table_name}")
            return generated_ids
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"✗ Database error inserting into {table_name}: {e}")
            raise
        finally:
            session.close()
    
    def insert_batch(self, table_name: str, data: List[Dict], batch_size: int = 100) -> List[int]:
        """
        Insert data in batches and return generated IDs
        """
        if not data:
            return []
        
        session = self.Session()
        all_generated_ids = []
        id_column = self.id_columns.get(table_name, f"{table_name[:-1]}_id")
        
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                batch_ids = []
                
                for record in batch:
                    record = {k: v for k, v in record.items() if v is not None}
                    record.pop(id_column, None)
                    
                    if not record:
                        continue
                    
                    columns = ', '.join(record.keys())
                    placeholders = ', '.join([f":{k}" for k in record.keys()])
                    
                    sql = text(f"""
                        INSERT INTO {table_name} ({columns}) 
                        VALUES ({placeholders})
                        RETURNING {id_column}
                    """)
                    
                    result = session.execute(sql, record)
                    batch_ids.append(result.scalar())
                
                session.commit()
                all_generated_ids.extend(batch_ids)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records into {table_name}")
            
            logger.info(f"✓ Total inserted {len(data)} records into {table_name}")
            return all_generated_ids
            
        except Exception as e:
            session.rollback()
            logger.error(f"✗ Batch insert failed: {e}")
            raise
        finally:
            session.close()
    
    def get_max_id(self, table_name: str, id_column: str) -> int:
        """Get maximum ID from a table"""
        session = self.Session()
        try:
            result = session.execute(text(f"SELECT COALESCE(MAX({id_column}), 0) FROM {table_name}"))
            max_id = result.scalar()
            return max_id or 0
        except Exception as e:
            logger.warning(f"Could not get max ID from {table_name}: {e}")
            return 0
        finally:
            session.close()
    
    def get_all_customers(self) -> List[Dict]:
        """Get all customers for order generation"""
        session = self.Session()
        try:
            result = session.execute(text("SELECT customer_id, full_name FROM customers"))
            customers = [{'customer_id': row[0], 'full_name': row[1]} for row in result]
            return customers
        except Exception as e:
            logger.error(f"Failed to get customers: {e}")
            return []
        finally:
            session.close()
    
    def get_all_products(self) -> List[Dict]:
        """Get all products for order item generation"""
        session = self.Session()
        try:
            result = session.execute(text("SELECT product_id, unit_price, category FROM products"))
            products = [{'product_id': row[0], 'unit_price': row[1], 'category': row[2]} for row in result]
            return products
        except Exception as e:
            logger.error(f"Failed to get products: {e}")
            return []
        finally:
            session.close()
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table"""
        session = self.Session()
        try:
            result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()
        except Exception as e:
            logger.warning(f"Could not get count for {table_name}: {e}")
            return 0
        finally:
            session.close()
    
    def get_recent_orders(self, limit: int = 100) -> List[Dict]:
        """Get recent orders for CDC simulation"""
        session = self.Session()
        try:
            result = session.execute(text(f"""
                SELECT order_id, customer_id, order_date, status, channel 
                FROM orders 
                ORDER BY order_date DESC 
                LIMIT {limit}
            """))
            orders = [{'order_id': row[0], 'customer_id': row[1], 'order_date': row[2], 
                      'status': row[3], 'channel': row[4]} for row in result]
            return orders
        finally:
            session.close()
    
    def truncate_table(self, table_name: str) -> None:
        """Truncate a table (remove all data)"""
        session = self.Session()
        try:
            session.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
            session.commit()
            logger.info(f"Truncated table: {table_name}")
        except Exception as e:
            session.rollback()
            logger.warning(f"Could not truncate {table_name}: {e}")
        finally:
            session.close()
    
    def truncate_all_tables(self) -> None:
        """Truncate all tables in correct dependency order"""
        tables = ['order_items', 'payments', 'returns', 'orders', 'customers', 'products']
        for table in tables:
            self.truncate_table(table)
        logger.info("All tables truncated")
    
    def execute_raw_sql(self, sql: str, params: Optional[Dict] = None) -> Any:
        """Execute raw SQL statement"""
        session = self.Session()
        try:
            result = session.execute(text(sql), params or {})
            session.commit()
            return result
        except Exception as e:
            session.rollback()
            logger.error(f"SQL execution failed: {e}")
            raise
        finally:
            session.close()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        session = self.Session()
        try:
            result = session.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
            """))
            return result.scalar()
        finally:
            session.close()
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get column information for a table"""
        session = self.Session()
        try:
            result = session.execute(text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """))
            return [{'column_name': row[0], 'data_type': row[1], 'is_nullable': row[2]} for row in result]
        finally:
            session.close()