#!/usr/bin/env python3
"""
DAG 2 — Gold Batch (toutes les 10 minutes)
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ── Config ─────────────────────────────────────────────────────────────────────
NESSIE_URL   = Variable.get("NESSIE_URL",      default_var="http://nessie:19120/api/v1")
SILVER_TABLES = ["customers", "products", "orders", "order_items", "payments", "returns"]

log = logging.getLogger("cdc_gold_dag")

def check_silver_ready(**ctx):
    """Vérifie que toutes les tables Silver existent dans Nessie"""
    expected = {f"silver.{t}" for t in SILVER_TABLES}
    for attempt in range(1, 21):
        try:
            r = requests.get(f"{NESSIE_URL}/trees/tree/main/entries", timeout=15)
            r.raise_for_status()
            entries = r.json().get("entries", [])
            found = {
                ".".join(e["name"]["elements"])
                for e in entries
                if e.get("name", {}).get("elements", [None])[0] == "silver"
            }
            missing = expected - found
            if not missing:
                log.info(f"✅ Silver OK — {len(found)} tables présentes")
                return True
            else:
                log.info(f"[{attempt}/20] ⏳ Silver pas encore prêt, manquantes: {missing}")
        except Exception as e:
            log.warning(f"Nessie check attempt {attempt}: {e}")
        time.sleep(30)
    raise RuntimeError("❌ Tables Silver absentes après 10 minutes")

default_args = {
    "owner": "cdc-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="cdc_gold_batch",
    description="Agrégations Gold toutes les 10 min",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cdc", "gold", "batch"],
) as dag:

    t_check_silver = PythonOperator(
        task_id="check_silver_ready",
        python_callable=check_silver_ready,
        execution_timeout=timedelta(minutes=10),
    )

    # Exécute le job Gold dans le conteneur existant
    t_gold = BashOperator(
        task_id="run_gold_batch",
        bash_command="""
        docker exec cdc-gold-processing /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse-gold/warehouse \
          /spark/jobs/gold_processing.py
        """,
        execution_timeout=timedelta(minutes=9),
    )

    t_done = EmptyOperator(task_id="gold_complete")

    t_check_silver >> t_gold >> t_done