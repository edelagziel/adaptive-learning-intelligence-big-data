# Demo Guide

This script is designed for a continuous local recording. Commands are written for Windows PowerShell from the repository root.

Approximate duration after images are already downloaded: 20-35 minutes, depending on machine resources and Spark startup time.

## 1. Start The Environment

Show the project root:

```powershell
Get-ChildItem
```

Create the shared Docker network:

```powershell
docker network inspect bigdata-net > $null 2>&1; if ($LASTEXITCODE -ne 0) { docker network create bigdata-net }
```

Start Spark/Iceberg/MinIO:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml up -d
```

Start Kafka:

```powershell
docker compose -f kafka-practice-env/docker-compose.yml up -d
```

Start Airflow:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml up -d
```

Verify containers:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Expected visible result: containers for `spark-iceberg`, `iceberg-rest`, `minio`, `kafka`, `airflow-apiserver`, `airflow-scheduler`, workers, PostgreSQL, and Redis are running or healthy.

## 2. Open Airflow

Open:

```text
http://localhost:8081
```

Login:

```text
airflow / airflow
```

Create the Spark serialization pool:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow pools set spark_iceberg_pool 1 "Serialize local Spark/Iceberg jobs"
```

Expected visible result: Airflow UI shows current DAGs after the scheduler parses them.

## 3. Run Environment Setup

Trigger:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow dags trigger ali_environment_setup
```

In the UI, show:

```text
setup_namespaces -> setup_bronze_tables -> setup_quality_tables -> setup_silver_tables -> setup_gold_tables -> setup_ml_tables -> setup_runtime_paths
```

Expected visible result: all setup tasks succeed. This DAG is manual and safe to rerun.

## 4. Run Batch Ingestion

Trigger:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow dags trigger ali_batch_ingestion
```

Expected visible result: `bronze_batch` succeeds and batch JSON files from `jasonData/` are loaded to Bronze.

## 5. Demonstrate Kafka Input

List Kafka topics:

```powershell
docker compose -f kafka-practice-env/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

Expected visible result: `learning-events`.

Run the producer if local Python and `kafka-python` are installed:

```powershell
python kafka-practice-env/producer/learning_events_producer.py
```

Show messages:

```powershell
docker compose -f kafka-practice-env/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic learning-events --from-beginning --max-messages 5
```

Expected visible result: JSON events with `event_type` set to `practice_submitted`.

Fallback: if local Python cannot run the producer, continue with the batch path and explain that the streaming producer requires host Python plus `kafka-python`.

## 6. Run Kafka Ingestion

Trigger:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow dags trigger ali_kafka_ingestion
```

Expected visible result: `kafka_to_bronze_streaming` succeeds and exits after draining available events.

## 7. Run Processing Pipeline

Trigger:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow dags trigger ali_processing_pipeline
```

Show the task chain:

```text
bronze_quality -> silver_transform -> late_arriving_feedback -> silver_quality -> gold_dimensions -> gold_facts -> gold_aggregations -> gold_quality -> ml_training
```

Expected visible result: tasks run in strict order. Quality and validation tasks block downstream work if they return a nonzero exit code.

## 8. Inspect Tables

Show namespaces:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg /opt/spark/bin/spark-sql -e "SHOW NAMESPACES IN demo"
```

Show Gold tables:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg /opt/spark/bin/spark-sql -e "SHOW TABLES IN demo.gold"
```

Show quality results:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg /opt/spark/bin/spark-sql -e "SELECT source_table, rule_name, status, failed_rows FROM demo.quality.silver_quality_results ORDER BY source_table, rule_name"
```

Show ML predictions:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg /opt/spark/bin/spark-sql -e "SELECT user_key, topic_key, session_id, model_version, struggle_probability, prediction_created_at FROM demo.gold.ml_learning_difficulty_predictions ORDER BY prediction_created_at DESC LIMIT 10"
```

Expected visible result: Bronze, Silver, Gold, quality, and ML prediction tables are queryable after successful DAG runs.

## 9. Export And Open Dashboard

Export dashboard JSON:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_DRIVER_PYTHON_OPTS /opt/spark/bin/spark-submit /home/iceberg/notebooks/notebooks/jobs/demo/export_dashboard_data_job.py
```

Start HTTP server:

```powershell
docker run --rm --name ali-dashboard -p 8090:8090 -v "${PWD}\iceberg-practice-env\notebooks\dashboard:/dashboard" --entrypoint python3 tabulario/spark-iceberg -m http.server 8090 --directory /dashboard --bind 0.0.0.0
```

Open:

```text
http://localhost:8090
```

Show sections:

- KPI cards.
- Weakest concepts.
- Learning progress.
- ML risk.
- Evidence gap.
- Quality.
- Pipeline status.

Expected visible result: dashboard renders real exported Gold, Quality, and ML data from `dashboard/data/dashboard_data.json`.

## 10. Stop Demo Services

Stop the dashboard server with `Ctrl+C`.

Stop Compose services while preserving data:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml down
docker compose -f kafka-practice-env/docker-compose.yml down
docker compose -f iceberg-practice-env/docker-compose.yml down
```

