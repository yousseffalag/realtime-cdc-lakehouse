#!/usr/bin/env python3
import os
import time
import logging
import random
from datetime import datetime
from db import DatabaseManager
from generators import customer_gen, product_gen, order_gen, order_item_gen, payment_gen, return_gen

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    DATABASE_URL = os.getenv(
        'DATABASE_URL',
        'postgresql+psycopg2://cdc_user:cdc_pass@postgres:5432/cdc_source'
    )
    
    db = DatabaseManager(DATABASE_URL)
    
    # Track existing IDs for incremental generation
    last_customer_id = db.get_max_id('customers', 'customer_id')
    last_product_id = db.get_max_id('products', 'product_id')
    last_order_id = db.get_max_id('orders', 'order_id')
    
    logger.info("="*60)
    logger.info("CDC DATA GENERATOR - CONTINUOUS MODE")
    logger.info(f"Starting with: Customers={last_customer_id}, Products={last_product_id}, Orders={last_order_id}")
    logger.info("="*60)
    
    batch_count = 0
    
    while True:
        try:
            batch_count += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"BATCH {batch_count} - {datetime.now().isoformat()}")
            logger.info(f"{'='*60}")
            
            # STEP 1: Generate new customers (0-5 per batch)
            logger.info("[CDC] Checking for new customers...")
            new_customers_count = random.randint(0, 5)
            if new_customers_count > 0:
                customers_data = customer_gen.generate_customers(new_customers_count)
                customer_ids = db.insert_data('customers', customers_data)
                
                # Store for order generation
                new_customers = []
                for i, customer_id in enumerate(customer_ids):
                    new_customers.append({
                        **customers_data[i],
                        'customer_id': customer_id
                    })
                logger.info(f"✓ Added {len(new_customers)} new customers")
            else:
                new_customers = []
            
            # STEP 2: Generate new products (0-10 per batch)
            logger.info("[CDC] Checking for new products...")
            new_products_count = random.randint(0, 10)
            if new_products_count > 0:
                products_data = product_gen.generate_products(new_products_count)
                product_ids = db.insert_data('products', products_data)
                
                new_products = []
                for i, product_id in enumerate(product_ids):
                    new_products.append({
                        **products_data[i],
                        'product_id': product_id
                    })
                logger.info(f"✓ Added {len(new_products)} new products")
            else:
                new_products = []
            
            # STEP 3: Get all customers (existing + new) for orders
            all_customers = db.get_all_customers()
            
            # STEP 4: Generate new orders (5-20 per batch)
            logger.info("[CDC] Generating new orders...")
            new_orders_count = random.randint(5, 20)
            orders_data = order_gen.generate_orders(all_customers, new_orders_count)
            
            if orders_data:
                order_ids = db.insert_data('orders', orders_data)
                
                new_orders = []
                for i, order_id in enumerate(order_ids):
                    new_orders.append({
                        **orders_data[i],
                        'order_id': order_id
                    })
                logger.info(f"✓ Added {len(new_orders)} new orders")
                
                # STEP 5: Generate order items for new orders
                all_products = db.get_all_products()
                logger.info("[CDC] Generating order items...")
                order_items_data = order_item_gen.generate_order_items(new_orders, all_products)
                if order_items_data:
                    db.insert_data('order_items', order_items_data)
                    logger.info(f"✓ Added {len(order_items_data)} new order items")
                
                # STEP 6: Generate payments for some orders
                logger.info("[CDC] Generating payments...")
                payments_data = payment_gen.generate_payments(new_orders)
                if payments_data:
                    db.insert_data('payments', payments_data)
                    logger.info(f"✓ Added {len(payments_data)} new payments")
                
                # STEP 7: Generate returns for some orders
                logger.info("[CDC] Generating returns...")
                returns_data = return_gen.generate_returns(new_orders)
                if returns_data:
                    db.insert_data('returns', returns_data)
                    logger.info(f"✓ Added {len(returns_data)} new returns")
            
            # Show current counts
            logger.info(f"\n📊 CURRENT TOTALS:")
            logger.info(f"   Customers: {db.get_table_count('customers')}")
            logger.info(f"   Products: {db.get_table_count('products')}")
            logger.info(f"   Orders: {db.get_table_count('orders')}")
            logger.info(f"   Order Items: {db.get_table_count('order_items')}")
            logger.info(f"   Payments: {db.get_table_count('payments')}")
            logger.info(f"   Returns: {db.get_table_count('returns')}")
            
            # Wait before next batch (simulate CDC stream)
            sleep_seconds = random.randint(5, 15)
            logger.info(f"\n⏳ Waiting {sleep_seconds} seconds before next CDC batch...")
            time.sleep(sleep_seconds)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️ CDC Generator stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Error in batch {batch_count}: {e}")
            logger.info(f"Waiting 10 seconds before retry...")
            time.sleep(10)
            continue


if __name__ == "__main__":
    main()