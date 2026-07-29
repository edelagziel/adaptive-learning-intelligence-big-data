# ALI Learning Data Analytics Dashboard

This is a read-only static demo dashboard for the ALI Big Data pipeline. It shows aggregated learner progress, weak concepts, evidence gaps, data quality status, and predicted learning risk from existing Iceberg tables.

The dashboard uses only plain HTML, CSS, JavaScript, and a generated local JSON file. It does not create, update, or delete Iceberg tables.

## Files

- `index.html` - dashboard page
- `styles.css` - local styling
- `dashboard.js` - local rendering logic
- `data/dashboard_data.json` - generated data artifact
- `../jobs/demo/export_dashboard_data_job.py` - read-only Spark export job

## Tables Read By The Export Job

Gold tables:

- `demo.gold.dim_learner`
- `demo.gold.dim_topic`
- `demo.gold.fact_learning_interaction`
- `demo.gold.fact_practice_attempt`
- `demo.gold.fact_learner_concept_state`
- `demo.gold.agg_learning_progress_daily`
- `demo.gold.agg_illusion_of_learning`
- `demo.gold.ml_learning_difficulty_features`
- `demo.gold.ml_learning_difficulty_predictions`

Quality tables:

- `demo.quality.bronze_quality_results`
- `demo.quality.silver_quality_results`
- `demo.quality.bronze_quarantine`
- `demo.quality.silver_quarantine`

Gold quality results are not persisted by the current pipeline, so Gold table readability is reported as availability, not as a Gold quality pass.

## Generate Dashboard Data

From the Windows project root:

```bash
docker compose -f iceberg-practice-env/docker-compose.yml exec spark-iceberg env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_DRIVER_PYTHON_OPTS /opt/spark/bin/spark-submit /home/iceberg/notebooks/notebooks/jobs/demo/export_dashboard_data_job.py
```

From `iceberg-practice-env`:

```bash
docker compose exec spark-iceberg env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_DRIVER_PYTHON_OPTS /opt/spark/bin/spark-submit /home/iceberg/notebooks/notebooks/jobs/demo/export_dashboard_data_job.py
```

The export job writes:

```text
iceberg-practice-env/notebooks/dashboard/data/dashboard_data.json
```

inside the container path:

```text
/home/iceberg/notebooks/notebooks/dashboard/data/dashboard_data.json
```

## Open In JupyterLab

The Spark/Jupyter container maps:

- host `iceberg-practice-env/notebooks`
- container `/home/iceberg/notebooks/notebooks`
- port `8888`

After JupyterLab is running and authenticated, open:

```text
http://localhost:8888/files/dashboard/index.html
```

You can also browse in JupyterLab to:

```text
/home/iceberg/notebooks/notebooks/dashboard/index.html
```

## Refresh Data

1. Run the export job.
2. Open or refresh `index.html`.
3. Use the dashboard's `Refresh data` button to reload `data/dashboard_data.json`.

The page adds a cache-busting query string when loading JSON, but a hard browser refresh can help if the browser keeps an older file.

## Empty Data Behavior

If a table is empty, the related dashboard section shows an empty-state message or `Not available`. The export job emits `status: "partial"` when optional dashboard sections cannot be read.

The dashboard never invents fake values. Missing metrics remain visibly unavailable.

## Troubleshooting

Missing JSON:

- Run the export job and confirm `dashboard/data/dashboard_data.json` exists.

Spark container not running:

- Start the existing `iceberg-practice-env` services, then rerun the export command.

Jupyter not running:

- Start the existing Spark/Jupyter service and open port `8888`.

Table unavailable:

- Run the approved setup and processing pipeline jobs first.
- Check the export job logs for the unavailable table and missing column details.

Browser caching:

- Click `Refresh data`.
- Hard-refresh the browser tab if needed.

## Read-Only Guarantee

The dashboard export job reads existing Iceberg Gold, Quality, and ML tables and writes only the local JSON dashboard artifact. It does not modify Iceberg schemas, tables, checkpoints, models, DAGs, or pipeline data.
