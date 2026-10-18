-- Privileges for the github-ci service principal (application ID below).
-- CI builds dbt into ci_silver / ci_gold, which it creates and owns, and the
-- prod bundle jobs run as this principal, so it also writes bronze and silver.
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG crypto TO `b8b58458-7c47-43f5-8343-f9988293ef0d`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA crypto.bronze TO `b8b58458-7c47-43f5-8343-f9988293ef0d`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA crypto.silver TO `b8b58458-7c47-43f5-8343-f9988293ef0d`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME crypto.bronze.checkpoints TO `b8b58458-7c47-43f5-8343-f9988293ef0d`;
GRANT READ FILES ON EXTERNAL LOCATION landing TO `b8b58458-7c47-43f5-8343-f9988293ef0d`;
-- Outside SQL: secret scope `crypto` READ, warehouse CAN_USE, entitlement allow-cluster-create.
-- No grant on gold: CI never touches the real Gold tables.
