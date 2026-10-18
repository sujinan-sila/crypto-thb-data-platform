"""Process whatever is waiting in Event Hubs, every 6 hours (availableNow)."""
from datetime import datetime, timedelta

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag

from include.assets import SILVER_CANDLES


@dag(
    dag_id="stream_runs",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,               # never two runs on the same checkpoint
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["streaming", "databricks"],
)
def stream_runs():
    DatabricksRunNowOperator(
        task_id="stream_bronze_silver",
        job_id="{{ var.value.job_stream_bronze_silver }}",
        outlets=[SILVER_CANDLES],
        pool="databricks_compute",   # one job cluster at a time, see airflow_settings.yaml
    )


stream_runs()
