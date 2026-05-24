#!/usr/bin/env python3
"""
DAG 1 — CDC Streaming (Bronze + Silver)
========================================
Se lance UNE SEULE FOIS au démarrage de la plateforme.
- Vérifie Debezium et enregistre le connecteur Postgres
- Lance Bronze en streaming continu (Kafka → Iceberg Bronze)
- Lance Silver en streaming continu (Bronze → Iceberg Silver)
- Les deux jobs restent vivants indéfiniment
- Ce DAG ne se re-schedule pas (@once)
"""
from __future__ import annotations

import json
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
DEBEZIUM_URL   = Variable.get("DEBEZIUM_URL",    default_var="http://debezium:8083")
NESSIE_URL     = Variable.get("NESSIE_URL",      default_var="http://nessie:19120/api/v1")
SPARK_MASTER   = Variable.get("SPARK_MASTER",    default_var="spark://spark-master:7077")
MINIO_KEY      = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
MINIO_SECRET   = Variable.get("MINIO_SECRET_KEY", default_var="hshsh83skskk8lsk8320ljsks73")
POSTGRES_HOST  = Variable.get("POSTGRES_HOST",   default_var="postgres")
POSTGRES_DB    = Variable.get("POSTGRES_DB",     default_var="cdc_db")
POSTGRES_USER  = Variable.get("POSTGRES_USER",   default_var="cdc_user")
POSTGRES_PASS  = Variable.get("POSTGRES_PASS",   default_var="1234")

CONNECTOR_NAME = "postgres-cdc-connector"
BRONZE_TABLES  = ["customers", "products", "orders", "order_items", "payments", "returns"]

SPARK_JARS = ",".join([
    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "/opt/spark/jars/kafka-clients-3.4.1.jar",
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "/opt/spark/jars/commons-pool2-2.11.1.jar",
    "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark/jars/iceberg-nessie-1.5.2.jar",
    "/opt/spark/jars/bundle-2.20.18.jar",
    "/opt/spark/jars/url-connection-client-2.20.18.jar",
])

SPARK_CONF = (
    f"--master {SPARK_MASTER} --deploy-mode client "
    "--conf spark.executor.memory=1g "
    "--conf spark.driver.memory=1g "
    "--conf spark.executor.cores=2 "
    "--conf spark.cores.max=2 "
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.nessie.NessieCatalog "
    "--conf spark.sql.catalog.iceberg.uri=http://nessie:19120/api/v1 "
    "--conf spark.sql.catalog.iceberg.ref=main "
    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
    "--conf spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000 "
    f"--conf spark.sql.catalog.iceberg.s3.access-key-id={MINIO_KEY} "
    f"--conf spark.sql.catalog.iceberg.s3.secret-access-key={MINIO_SECRET} "
    "--conf spark.sql.catalog.iceberg.s3.path-style-access=true "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    f"--conf spark.hadoop.fs.s3a.access.key={MINIO_KEY} "
    f"--conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET} "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "--conf spark.driver.extraClassPath=/opt/spark/jars/* "
    "--conf spark.executor.extraClassPath=/opt/spark/jars/* "
    f"--jars {SPARK_JARS} "
)

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": POSTGRES_HOST,
        "database.port": "5432",
        "database.user": POSTGRES_USER,
        "database.password": POSTGRES_PASS,
        "database.dbname": POSTGRES_DB,
        "database.server.name": "postgres",
        "topic.prefix": "postgres",
        "table.include.list": "public.customers,public.products,public.orders,public.order_items,public.payments,public.returns",
        "plugin.name": "pgoutput",
        "publication.autocreate.mode": "filtered",
        "slot.name": "debezium_slot",
        "snapshot.mode": "initial",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "heartbeat.interval.ms": "10000",
        "decimal.handling.mode": "string",
    },
}

log = logging.getLogger("cdc_streaming_dag")


# ── Task functions ─────────────────────────────────────────────────────────────

def check_debezium_health(**ctx):
    for attempt in range(1, 21):
        try:
            r = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=10)
            if r.status_code == 200:
                log.info(f"Debezium healthy (attempt {attempt})")
                return True
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt}/20 — {e}")
        time.sleep(15)
    raise RuntimeError("Debezium not healthy after 20 attempts")


def register_connector(**ctx):
    headers  = {"Content-Type": "application/json"}
    existing = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=10).json()
    log.info(f"Existing connectors: {existing}")

    # Supprimer les connecteurs parasites
    for name in [c for c in existing if c != CONNECTOR_NAME]:
        log.warning(f"Deleting stale connector: {name}")
        requests.delete(f"{DEBEZIUM_URL}/connectors/{name}", timeout=15)

    # Supprimer le connecteur cible pour repartir propre
    if CONNECTOR_NAME in existing:
        log.info(f"Deleting existing '{CONNECTOR_NAME}' for clean re-create")
        requests.delete(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}", timeout=15)
        time.sleep(5)

    log.info(f"Creating connector '{CONNECTOR_NAME}'...")
    r = requests.post(
        f"{DEBEZIUM_URL}/connectors",
        data=json.dumps(CONNECTOR_CONFIG),
        headers=headers,
        timeout=15,
    )
    if not r.ok:
        log.error(f"Response: {r.text}")
        r.raise_for_status()
    log.info(f"Connector created ({r.status_code})")


def check_connector_running(**ctx):
    for attempt in range(1, 31):
        try:
            r = requests.get(
                f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10
            )
            r.raise_for_status()
            status = r.json()
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            tasks           = status.get("tasks", [])
            task_states     = [t.get("state") for t in tasks]
            log.info(f"[{attempt}] Connector: {connector_state} | Tasks: {task_states}")

            for i, task in enumerate(tasks):
                if task.get("state") == "FAILED":
                    requests.post(
                        f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/tasks/{i}/restart",
                        timeout=10,
                    )

            if connector_state == "RUNNING" and all(s == "RUNNING" for s in task_states):
                log.info("Connector RUNNING")
                return True
        except Exception as e:
            log.warning(f"Attempt {attempt}: {e}")
        time.sleep(10)
    raise RuntimeError("Connector did not reach RUNNING state")


def wait_for_bronze(**ctx):
    expected = {f"bronze.{t}" for t in BRONZE_TABLES}
    for attempt in range(1, 41):
        try:
            r = requests.get(f"{NESSIE_URL}/trees/tree/main/entries", timeout=15)
            entries = r.json().get("entries", [])
            found = {
                ".".join(e["name"]["elements"])
                for e in entries
                if e.get("name", {}).get("elements", [None])[0] == "bronze"
            }
            missing = expected - found
            log.info(f"[{attempt}] Bronze: {found} | Missing: {missing}")
            if not missing:
                log.info("Toutes les tables Bronze sont dans Nessie")
                return True
        except Exception as e:
            log.warning(f"Attempt {attempt}: {e}")
        time.sleep(30)
    raise RuntimeError("Bronze tables absentes après 20 min")


def wait_for_silver(**ctx):
    expected = {f"silver.{t}" for t in BRONZE_TABLES}
    for attempt in range(1, 41):
        try:
            r = requests.get(f"{NESSIE_URL}/trees/tree/main/entries", timeout=15)
            entries = r.json().get("entries", [])
            found = {
                ".".join(e["name"]["elements"])
                for e in entries
                if e.get("name", {}).get("elements", [None])[0] == "silver"
            }
            missing = expected - found
            log.info(f"[{attempt}] Silver: {found} | Missing: {missing}")
            if not missing:
                log.info("Toutes les tables Silver sont dans Nessie")
                return True
        except Exception as e:
            log.warning(f"Attempt {attempt}: {e}")
        time.sleep(30)
    raise RuntimeError("Silver tables absentes après 20 min")


# ── DAG ────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "cdc-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="cdc_streaming",
    description="Lance Bronze + Silver en streaming continu (une seule fois au démarrage)",
    schedule_interval="@once",          # <-- se lance UNE SEULE FOIS
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cdc", "streaming", "bronze", "silver"],
) as dag:

    t_health = PythonOperator(
        task_id="check_debezium_health",
        python_callable=check_debezium_health,
    )

    t_register = PythonOperator(
        task_id="register_debezium_connector",
        python_callable=register_connector,
    )

    t_status = PythonOperator(
        task_id="check_connector_running",
        python_callable=check_connector_running,
    )

    # Bronze — streaming continu, reste vivant indéfiniment
    t_bronze = BashOperator(
        task_id="start_bronze_streaming",
        bash_command=(
            f"nohup /opt/spark/bin/spark-submit {SPARK_CONF}"
            "--conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse-bronze/warehouse "
            "/spark/jobs/bronze_ingestion.py "
            "> /tmp/bronze.log 2>&1 & "
            "echo $! > /tmp/bronze.pid && "
            "echo \"Bronze started PID=$(cat /tmp/bronze.pid)\""
        ),
    )

    t_wait_bronze = PythonOperator(
        task_id="wait_bronze_data_ready",
        python_callable=wait_for_bronze,
        execution_timeout=timedelta(minutes=30),
    )

    # Silver — streaming continu, reste vivant indéfiniment
    t_silver = BashOperator(
        task_id="start_silver_streaming",
        bash_command=(
            f"nohup /opt/spark/bin/spark-submit {SPARK_CONF}"
            "--conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse-silver/warehouse "
            "/spark/jobs/silver_processing.py "
            "> /tmp/silver.log 2>&1 & "
            "echo $! > /tmp/silver.pid && "
            "echo \"Silver started PID=$(cat /tmp/silver.pid)\""
        ),
    )

    t_wait_silver = PythonOperator(
        task_id="wait_silver_data_ready",
        python_callable=wait_for_silver,
        execution_timeout=timedelta(minutes=30),
    )

    t_done = EmptyOperator(task_id="streaming_ready")

    (
        t_health
        >> t_register
        >> t_status
        >> t_bronze
        >> t_wait_bronze
        >> t_silver
        >> t_wait_silver
        >> t_done
    )