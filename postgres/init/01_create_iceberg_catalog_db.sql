-- Separate database for Iceberg's JDBC catalog metadata (table pointers,
-- namespace properties). Kept apart from Airflow's own metadata database.
CREATE DATABASE iceberg_catalog;
