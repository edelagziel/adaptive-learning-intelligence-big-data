# Architecture

ALI is split into three local Docker Compose environments that communicate through the shared external Docker network `bigdata-net`.

| Directory | Role | Compose file |
|---|---|---|
| `iceberg-practice-env` | Spark processing, JupyterLab, Iceberg REST, MinIO, dashboard files | `docker-compose.yml` |
| `kafka-practice-env` | Kafka broker, topic initialization, learning event producer | `docker-compose.yml` |
| `airflow-practice-env` | Airflow orchestration with Docker SDK execution | `docker-compose.yaml` |

## Component Responsibilities

- `spark-iceberg`: runs all Spark applications and exposes JupyterLab.
- `iceberg-rest`: serves the Iceberg REST catalog.
- `minio`: stores Iceberg data files.
- `mc`: initializes the local MinIO warehouse bucket.
- `kafka`: hosts the `learning-events` topic.
- `kafka-init`: creates the Kafka topic if it does not exist.
- `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer`: run Airflow.
- `postgres`: Airflow metadata database.
- `redis`: Airflow Celery broker.

## Storage And Catalog Separation

Iceberg table data is stored physically in MinIO. Iceberg metadata and snapshots are coordinated through the Iceberg REST catalog. Spark jobs talk to the catalog and storage from inside the `spark-iceberg` container.

```mermaid
flowchart LR
    Spark[spark-iceberg container] --> Rest[Iceberg REST catalog]
    Spark --> MinIO[(MinIO object storage)]
    Rest --> MinIO
    MinIO --> Warehouse[s3://warehouse/]
```

## Batch Flow

```mermaid
flowchart LR
    Sources[jasonData JSON files] --> Batch[bronze_batch_job.py]
    Batch --> Bronze[demo.bronze.*]
    Bronze --> BQ[bronze_quality_job.py]
    BQ --> Quarantine[demo.quality.bronze_quarantine]
    BQ --> Results[demo.quality.bronze_quality_results]
```

Batch input files:

- `jasonData/learner_profiles.json`
- `jasonData/question_bank.json`
- `jasonData/reference_materials.json`
- `jasonData/learning_events_final.json`
- `jasonData/learning_feedback.json`

## Streaming Flow

```mermaid
flowchart LR
    Producer[learning_events_producer.py] --> Kafka[Kafka topic: learning-events]
    Kafka --> Streaming[kafka_to_bronze_streaming_job.py]
    Streaming --> BronzeEvents[demo.bronze.learning_events]
```

The active producer emits `event_type = "practice_submitted"` events to Kafka at `localhost:9092`. Spark reads Kafka inside Docker through `kafka:29092`. The Kafka ingestion DAG runs every five minutes and the Spark job uses bounded available-now behavior.

## Orchestration Flow

```mermaid
flowchart TD
    Setup[ali_environment_setup] --> BatchDag[ali_batch_ingestion]
    KafkaDag[ali_kafka_ingestion] --> Processing[ali_processing_pipeline]
    BatchDag --> Processing
    Processing --> Export[Manual dashboard export]
    Export --> Dashboard[Dashboard at localhost:8090]
```

Current DAGs:

- `ali_environment_setup`: manual setup of namespaces, tables, and runtime paths.
- `ali_batch_ingestion`: daily batch load at 01:00 Asia/Jerusalem.
- `ali_kafka_ingestion`: bounded Kafka drain every five minutes.
- `ali_processing_pipeline`: hourly processing at minute 15.

All Spark and setup tasks use `spark_iceberg_pool`. Configure it with one slot in Airflow to serialize local Spark execution.

## Processing Flow

```mermaid
flowchart LR
    Bronze[Bronze] --> BronzeQuality[Bronze Quality]
    BronzeQuality --> Silver[Silver Transform]
    Silver --> Late[Late Arrival Validation]
    Late --> SilverQuality[Silver Quality]
    SilverQuality --> Dimensions[Gold Dimensions]
    Dimensions --> Facts[Gold Facts]
    Facts --> Aggregations[Gold Aggregations]
    Aggregations --> GoldQuality[Gold Quality]
    Aggregations --> ML[ML Training and Predictions]
```

Quality gates write results and quarantine rows. Downstream jobs filter unresolved quarantined source rows before writing curated Silver and Gold outputs.

## Dashboard Flow

```mermaid
flowchart LR
    Gold[Gold tables] --> Export[export_dashboard_data_job.py]
    Quality[Quality tables] --> Export
    Predictions[ML predictions] --> Export
    Export --> Json[dashboard/data/dashboard_data.json]
    Json --> Dashboard[HTML/CSS/JS dashboard]
```

The dashboard is static and read-only. It displays real exported aggregate/demo-safe data from Gold, Quality, and ML outputs.

