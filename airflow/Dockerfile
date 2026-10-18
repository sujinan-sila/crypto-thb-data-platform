FROM astrocrpublic.azurecr.io/runtime:3.3-9

# dbt in its own virtualenv so its dependencies never clash with Airflow's
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-databricks==1.12.7
