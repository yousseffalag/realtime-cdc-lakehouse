#!/bin/bash
# Démarrer le traitement Gold en arrière-plan
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.cores.max=2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.iceberg.uri=http://nessie:19120/api/v1 \
  --conf spark.sql.catalog.iceberg.ref=main \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse-gold/warehouse \
  --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.iceberg.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.iceberg.s3.secret-access-key=hshsh83skskk8lsk8320ljsks73 \
  --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=hshsh83skskk8lsk8320ljsks73 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.executorEnv.AWS_REGION=us-east-1 \
  --conf spark.executorEnv.AWS_ACCESS_KEY_ID=minioadmin \
  --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=hshsh83skskk8lsk8320ljsks73 \
  --conf spark.driver.extraClassPath=/opt/spark/jars/* \
  --conf spark.executor.extraClassPath=/opt/spark/jars/* \
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/commons-pool2-2.11.1.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/iceberg-nessie-1.5.2.jar,/opt/spark/jars/bundle-2.20.18.jar,/opt/spark/jars/url-connection-client-2.20.18.jar \
  /spark/jobs/gold_processing.py &

# Garder le conteneur vivant
sleep infinity