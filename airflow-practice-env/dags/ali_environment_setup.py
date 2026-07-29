"""Manual environment setup DAG for ALI Iceberg infrastructure."""

from __future__ import annotations

import logging
import posixpath
from datetime import timedelta
from typing import Optional

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


logger = logging.getLogger(__name__)

DAG_ID = "ali_environment_setup"
SPARK_CONTAINER_NAME = "spark-iceberg"
SETUP_JOBS_DIR = "/home/iceberg/notebooks/notebooks/jobs/setup"

REGULAR_TASK_TIMEOUT = timedelta(minutes=30)

DEFAULT_ARGS = {
    "owner": "ali",
    "depends_on_past": False,
    "retries": 0,
}


def _validate_job_filename(job_filename: str) -> None:
    if not isinstance(job_filename, str) or not job_filename:
        raise AirflowException("job_filename must be a non-empty string.")

    if job_filename != posixpath.basename(job_filename):
        raise AirflowException(f"Invalid job_filename with path component: {job_filename!r}")

    if "\\" in job_filename or "/" in job_filename:
        raise AirflowException(f"Invalid job_filename with path separator: {job_filename!r}")

    if not job_filename.endswith("_job.py"):
        raise AirflowException(f"Invalid job_filename suffix: {job_filename!r}")


def _log_exec_chunk(data: Optional[bytes], log_method) -> None:
    if data is None:
        return

    text = data.decode("utf-8", errors="replace")
    if not text.strip():
        return

    for line in text.rstrip().splitlines():
        if line.strip():
            log_method(line)


def _run_container_command(job_filename: str, command: list[str]) -> None:
    _validate_job_filename(job_filename)

    client = None
    exec_id = None
    try:
        try:
            import docker
        except Exception as exc:
            raise AirflowException("Docker SDK is unavailable in the Airflow worker.") from exc

        try:
            client = docker.from_env()
        except Exception as exc:
            raise AirflowException("Unable to create Docker client from environment.") from exc

        try:
            container = client.containers.get(SPARK_CONTAINER_NAME)
        except Exception as exc:
            raise AirflowException(f"Container {SPARK_CONTAINER_NAME!r} was not found.") from exc

        try:
            container.reload()
        except Exception as exc:
            raise AirflowException(f"Unable to refresh container state for {SPARK_CONTAINER_NAME!r}.") from exc

        if container.status != "running":
            raise AirflowException(
                f"Container {SPARK_CONTAINER_NAME!r} is not running; status={container.status!r}."
            )

        logger.info("Starting setup job in %s: %s", SPARK_CONTAINER_NAME, job_filename)
        try:
            exec_response = client.api.exec_create(
                container.id,
                command,
                stdout=True,
                stderr=True,
            )
            exec_id = exec_response["Id"]
        except Exception as exc:
            raise AirflowException(f"Failed to create Docker exec for {job_filename!r}.") from exc

        try:
            output_stream = client.api.exec_start(
                exec_id,
                stream=True,
                demux=True,
            )
            for chunk in output_stream:
                if isinstance(chunk, tuple):
                    stdout_chunk, stderr_chunk = chunk
                    _log_exec_chunk(stdout_chunk, logger.info)
                    _log_exec_chunk(stderr_chunk, logger.error)
                else:
                    _log_exec_chunk(chunk, logger.info)
        except Exception as exc:
            raise AirflowException(f"Failed while streaming Docker exec output for {job_filename!r}.") from exc

        try:
            inspect_result = client.api.exec_inspect(exec_id)
        except Exception as exc:
            raise AirflowException(f"Failed to inspect Docker exec result for {job_filename!r}.") from exc

        exit_code = inspect_result.get("ExitCode")
        logger.info("Setup job finished | job=%s exit_code=%s", job_filename, exit_code)
        if exit_code != 0:
            raise AirflowException(f"Setup job {job_filename!r} failed with exit code {exit_code}.")
    finally:
        if client is not None:
            client.close()


def run_spark_setup_job(job_filename: str) -> None:
    command = [
        "env",
        "-u",
        "PYSPARK_DRIVER_PYTHON",
        "-u",
        "PYSPARK_DRIVER_PYTHON_OPTS",
        "/opt/spark/bin/spark-submit",
        f"{SETUP_JOBS_DIR}/{job_filename}",
    ]
    _run_container_command(job_filename, command)


def run_python_setup_job(job_filename: str) -> None:
    command = [
        "python3",
        f"{SETUP_JOBS_DIR}/{job_filename}",
    ]
    _run_container_command(job_filename, command)


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jerusalem"),
    catchup=False,
    max_active_runs=1,
    tags=["ali", "setup", "iceberg", "airflow"],
) as dag:
    setup_namespaces = PythonOperator(
        task_id="setup_namespaces",
        python_callable=run_spark_setup_job,
        op_kwargs={"job_filename": "setup_namespaces_job.py"},
        execution_timeout=REGULAR_TASK_TIMEOUT,
        do_xcom_push=False,
    )

    setup_bronze_tables = PythonOperator(
        task_id="setup_bronze_tables",
        python_callable=run_spark_setup_job,
        op_kwargs={"job_filename": "setup_bronze_tables_job.py"},
        execution_timeout=REGULAR_TASK_TIMEOUT,
        do_xcom_push=False,
    )

    setup_quality_tables = PythonOperator(
        task_id="setup_quality_tables",
        python_callable=run_spark_setup_job,
        op_kwargs={"job_filename": "setup_quality_tables_job.py"},
        execution_timeout=REGULAR_TASK_TIMEOUT,
        do_xcom_push=False,
    )

    setup_silver_tables = PythonOperator(
        task_id="setup_silver_tables",
        python_callable=run_spark_setup_job,
        op_kwargs={"job_filename": "setup_silver_tables_job.py"},
        execution_timeout=REGULAR_TASK_TIMEOUT,
        do_xcom_push=False,
    )

    setup_gold_tables = PythonOperator(
        task_id="setup_gold_tables",
        python_callable=run_spark_setup_job,
        op_kwargs={"job_filename": "setup_gold_tables_job.py"},
        execution_timeout=REGULAR_TASK_TIMEOUT,
        do_xcom_push=False,
    )

    setup_ml_tables = PythonOperator(
        task_id="setup_ml_tables",
        python_callable=run_spark_setup_job,
        op_kwargs={"job_filename": "setup_ml_tables_job.py"},
        execution_timeout=REGULAR_TASK_TIMEOUT,
        do_xcom_push=False,
    )

    setup_runtime_paths = PythonOperator(
        task_id="setup_runtime_paths",
        python_callable=run_python_setup_job,
        op_kwargs={"job_filename": "setup_runtime_paths_job.py"},
        execution_timeout=REGULAR_TASK_TIMEOUT,
        do_xcom_push=False,
    )

    (
        setup_namespaces
        >> setup_bronze_tables
        >> setup_quality_tables
        >> setup_silver_tables
        >> setup_gold_tables
        >> setup_ml_tables
        >> setup_runtime_paths
    )
