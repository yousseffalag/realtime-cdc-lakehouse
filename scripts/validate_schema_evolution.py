import sys
import os

# Adjust python path so we can import spark common configurations
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from spark.common.spark_session import get_spark_session

def run_schema_evolution_validation():
    """
    Executes a series of SQL mutations on an Iceberg table in real-time
    to demonstrate and validate schema evolution capabilities.
    """
    print("=============================================================")
    # 1. Initialize Spark Session
    print("[*] Initializing Spark Session...")
    spark = get_spark_session("Schema_Evolution_Validation")
    spark.sparkContext.setLogLevel("WARN")
    
    # Switch catalog context to the default lakehouse
    spark.sql("USE lakehouse")
    
    # 2. Setup Temporary Validation Table
    test_table = "silver.test_schema_evolution"
    print(f"[*] Resetting test table: {test_table}")
    spark.sql(f"DROP TABLE IF EXISTS {test_table}")
    
    print(f"[*] Creating table {test_table} with initial schema: (id INT, name STRING, age INT)")
    spark.sql(f"""
        CREATE TABLE {test_table} (
            id INT,
            name STRING,
            age INT
        ) USING iceberg
    """)
    
    # 3. Insert Initial Data
    print("[*] Inserting initial dataset...")
    spark.sql(f"INSERT INTO {test_table} VALUES (1, 'Alice', 25), (2, 'Bob', 30)")
    
    print("\n---> Initial Table Content:")
    spark.sql(f"SELECT * FROM {test_table}").show()
    
    # 4. MUTATION 1: Add a Column
    print("-------------------------------------------------------------")
    print("[*] MUTATION 1: Adding column 'email' (STRING) to table...")
    spark.sql(f"ALTER TABLE {test_table} ADD COLUMNS (email STRING)")
    
    print("[*] Inserting a record with the new schema...")
    spark.sql(f"INSERT INTO {test_table} VALUES (3, 'Charlie', 35, 'charlie@example.com')")
    
    print("\n---> Table Content after ADD COLUMN:")
    print("(Note how older records gracefully default to NULL for the new column)")
    spark.sql(f"SELECT * FROM {test_table} ORDER BY id ASC").show()
    
    # 5. MUTATION 2: Rename a Column
    print("-------------------------------------------------------------")
    print("[*] MUTATION 2: Renaming column 'name' to 'full_name'...")
    spark.sql(f"ALTER TABLE {test_table} RENAME COLUMN name TO full_name")
    
    print("\n---> Table Content after RENAME COLUMN:")
    print("(Historical values are mapped instantly to the renamed column without rewriting data files)")
    spark.sql(f"SELECT * FROM {test_table} ORDER BY id ASC").show()
    
    # 6. MUTATION 3: Drop a Column
    print("-------------------------------------------------------------")
    print("[*] MUTATION 3: Dropping column 'age' from table...")
    spark.sql(f"ALTER TABLE {test_table} DROP COLUMN age")
    
    print("\n---> Final Table Content after DROP COLUMN:")
    spark.sql(f"SELECT * FROM {test_table} ORDER BY id ASC").show()
    
    print("=============================================================")
    print("[+] SCHEMA EVOLUTION VALIDATION SUCCESSFUL!")
    print(f"[i] Table '{test_table}' is kept on MinIO so you can query it via Trino to check cross-compatibility.")
    print("=============================================================")

if __name__ == "__main__":
    run_schema_evolution_validation()
