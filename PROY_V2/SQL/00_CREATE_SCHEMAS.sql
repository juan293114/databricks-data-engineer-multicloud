-- =============================================================
-- 00_CREATE_SCHEMAS.sql
-- Arquitectura Medallón - Databricks / Unity Catalog
-- =============================================================
-- Ajustar CATALOG si el workspace utiliza un catálogo distinto.

CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

-- Las tablas se crean/registran desde los notebooks mediante saveAsTable()
-- para conservar los esquemas inferidos de los archivos fuente.
-- Bronze habilita CDF antes de la ingesta Auto Loader.
