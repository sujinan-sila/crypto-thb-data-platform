"""Build silver (batch) and gold with dbt once the daily batch data has landed."""
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig

from include.assets import BRONZE_BATCH

DBT_DIR = "/usr/local/airflow/dbt"

dbt_gold = DbtDag(
    dag_id="dbt_gold",
    # dbt/target is shared with dbt runs on Windows; its partial-parse cache has
    # Windows paths that the Linux container can't read, so always parse fresh
    project_config=ProjectConfig(DBT_DIR, partial_parse=False),
    profile_config=ProfileConfig(
        profile_name="crypto_thb",
        target_name="dev",
        profiles_yml_filepath=f"{DBT_DIR}/profiles.yml",
    ),
    execution_config=ExecutionConfig(dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"),
    schedule=[BRONZE_BATCH],         # data-aware: runs when bronze has been refreshed
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["dbt"],
)
