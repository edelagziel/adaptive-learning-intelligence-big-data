# Troubleshooting

## Docker Network Missing

Symptom: services fail to start with a missing `bigdata-net` error.

Fix:

```powershell
docker network inspect bigdata-net > $null 2>&1; if ($LASTEXITCODE -ne 0) { docker network create bigdata-net }
```

## Container Not Running

Check:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Logs:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml logs spark-iceberg
docker compose -f kafka-practice-env/docker-compose.yml logs kafka
docker compose -f airflow-practice-env/docker-compose.yaml logs airflow-scheduler
```

## Airflow DAG Not Visible

Check Airflow services:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml ps
```

Check parser logs:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml logs airflow-dag-processor
```

List DAGs:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow dags list
```

## Airflow Pool Missing

Symptom: tasks stay queued because `spark_iceberg_pool` does not exist.

Fix:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow pools set spark_iceberg_pool 1 "Serialize local Spark/Iceberg jobs"
```

## Task Stuck Queued

Check the pool:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml exec airflow-apiserver airflow pools list
```

Check worker logs:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml logs airflow-worker
```

The DAGs intentionally serialize Spark execution through a one-slot pool, so only one setup/Spark task should run at a time.

## Spark Container Name Mismatch

Airflow DAGs execute jobs inside the Docker container named `spark-iceberg`. Verify it exists:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}" | Select-String spark-iceberg
```

If it is missing, start the processing environment:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml up -d
```

## Jupyter Access Issues

JupyterLab is exposed at:

```text
http://localhost:8888
```

If the browser requests a token, inspect the Spark container logs:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml logs spark-iceberg
```

If `/files/dashboard/index.html` returns a permission or token page, use the temporary dashboard server instead:

```powershell
docker run --rm --name ali-dashboard -p 8090:8090 -v "${PWD}\iceberg-practice-env\notebooks\dashboard:/dashboard" --entrypoint python3 tabulario/spark-iceberg -m http.server 8090 --directory /dashboard --bind 0.0.0.0
```

Open:

```text
http://localhost:8090
```

## Dashboard JSON Not Loaded

Regenerate the dashboard JSON:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_DRIVER_PYTHON_OPTS /opt/spark/bin/spark-submit /home/iceberg/notebooks/notebooks/jobs/demo/export_dashboard_data_job.py
```

Confirm the file exists:

```powershell
Test-Path iceberg-practice-env/notebooks/dashboard/data/dashboard_data.json
```

Hard-refresh the browser to avoid stale cached JavaScript or JSON.

## Port 8090 Already In Use

Stop the previous dashboard server with `Ctrl+C` in the terminal that started it.

If a stale container exists:

```powershell
docker ps --filter "name=ali-dashboard"
```

Stop it:

```powershell
docker stop ali-dashboard
```

## MinIO Not Ready

Check MinIO:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml ps minio
docker compose -f iceberg-practice-env/docker-compose.yml logs minio
```

Open:

```text
http://localhost:9001
```

Login with the local demo account:

```text
admin / password
```

## Kafka Topic Unavailable

Check topic initialization:

```powershell
docker compose -f kafka-practice-env/docker-compose.yml logs kafka-init
```

List topics:

```powershell
docker compose -f kafka-practice-env/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

Expected topic:

```text
learning-events
```

## Kafka Producer Import Error

The host producer requires local Python and `kafka-python`.

Install only if you plan to run the producer from the host:

```powershell
python -m pip install kafka-python
```

Then run:

```powershell
python kafka-practice-env/producer/learning_events_producer.py
```

## Spark SQL Inspection Fails

Confirm the Spark/Iceberg environment is running:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml ps
```

Confirm namespaces after setup:

```powershell
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg /opt/spark/bin/spark-sql -e "SHOW NAMESPACES IN demo"
```

If namespaces are missing, run `ali_environment_setup`.

## Stale Browser Cache

Symptoms: dashboard layout or values do not match the latest export.

Fix:

1. Rerun the dashboard export job.
2. Hard-refresh the browser.
3. Restart the temporary dashboard server if needed.

## Safe Shutdown

Stop the dashboard server with `Ctrl+C`.

Preserve data:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml down
docker compose -f kafka-practice-env/docker-compose.yml down
docker compose -f iceberg-practice-env/docker-compose.yml down
```

Remove Compose-managed volumes only when you intentionally want to reset local service state:

```powershell
docker compose -f airflow-practice-env/docker-compose.yaml down -v
docker compose -f kafka-practice-env/docker-compose.yml down -v
docker compose -f iceberg-practice-env/docker-compose.yml down -v
```

