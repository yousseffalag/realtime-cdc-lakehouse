# 🚀 Real-time CDC Data Generator

This module is a high-performance synthetic data generator designed to simulate a realistic stream of e-commerce activities for testing Change Data Capture (CDC) pipelines. It populates a PostgreSQL database with a continuous flow of `INSERT`, `UPDATE`, and `DELETE` operations.

## 🏗️ Architecture

The generator follows a modular architecture to ensure consistency and ease of extension:

-   **`main.py`**: The entry point. It manages the continuous loop, database sessions, and executes SQL operations.
-   **`router.py`**: The decision engine. It uses weighted probabilities to decide which operation (Insert/Update/Delete) to perform on which table.
-   **`generators/`**: Contains specialized classes for each entity (Customers, Products, Orders, etc.).
-   **`state.py`**: An in-memory tracker that stores recently created IDs (e.g., valid `customer_id`s) to ensure that generated orders and payments have valid foreign keys.
-   **`utils/`**: Helper modules for random data generation and time handling.

## 📊 Supported Entities

The generator simulates activities across the following Medallion Architecture source tables:

| Entity | Operations | Description |
| :--- | :--- | :--- |
| **Customers** | INSERT, UPDATE, DELETE | Generates profiles with segments and geographic data. |
| **Products** | INSERT, UPDATE | Simulates new product launches and price changes. |
| **Orders** | INSERT, UPDATE, DELETE | Core transactional entity linking customers and products. |
| **Order Items** | INSERT | Line items for existing orders. |
| **Payments** | INSERT | Simulates payment processing for orders. |
| **Returns** | INSERT | Simulates customer returns for delivered items. |

## ⚙️ Configuration

The generator is highly configurable via environment variables or `config/settings.py`:

-   **Probabilities**: Adjust `CUSTOMER_INSERT_PROB`, `ORDER_INSERT_PROB`, etc., to change the frequency of different events.
-   **Database**: Connection parameters for the source PostgreSQL instance.
-   **Timing**: The interval between generated batches can be tuned for throughput testing.

## 🛠️ How to Run

### Using Docker (Recommended)
The generator is designed to run as a containerized service within the CDC platform:

```bash
# Build and start the generator
docker compose up -d --build data-generator

# Monitor the stream of events
docker compose logs -f data-generator
```

### Running Locally
If you want to run it outside of Docker for debugging:

```bash
cd data-generator
pip install -r requirements.txt
python main.py
```

## 💡 Key Features
-   **Foreign Key Integrity**: Uses the `State` class to ensure that an `order` always belongs to an existing `customer`.
-   **Postgres Sync**: Automatically captures `SERIAL` IDs returned from Postgres to keep the generator's state synchronized with the actual database.
-   **Realistic Flow**: Simulates a lifecycle (Customer -> Order -> Payment -> Shipping Update).
