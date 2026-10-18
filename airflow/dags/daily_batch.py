"""ADF pulls yesterday's API data into ADLS, then Auto Loader loads it into bronze."""
from datetime import datetime, timedelta

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.sdk import dag

from include.assets import BRONZE_BATCH


@dag(
    dag_id="daily_batch",
    schedule="30 0 * * *",          # 00:30 UTC, after the UTC day has closed
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["batch", "adf", "databricks"],
)
def daily_batch():
    ingest = AzureDataFactoryRunPipelineOperator(
        task_id="adf_ingest",
        pipeline_name="pl_ingest_daily",
        resource_group_name="rg-cryptothb7",
        factory_name="adf-cryptothb7",
        # Airflow 3: a cron run's logical date is the run time itself, so the
        # 00:30 run on the 15th has ds = 15th; the full day we want is the 14th
        parameters={"day": "{{ macros.ds_add(ds, -1) }}"},
        wait_for_termination=True,
    )
    to_bronze = DatabricksRunNowOperator(
        task_id="landing_to_bronze",
        job_id="{{ var.value.job_landing_to_bronze }}",
        outlets=[BRONZE_BATCH],
    )
    ingest >> to_bronze


daily_batch()
