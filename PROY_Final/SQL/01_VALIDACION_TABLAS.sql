-- =============================================================
-- 01_VALIDACION_TABLAS.sql
-- =============================================================

SHOW TABLES IN workspace.bronze;
SHOW TABLES IN workspace.silver;
SHOW TABLES IN workspace.gold;

SELECT COUNT(*) AS registros FROM workspace.bronze.sales;
SELECT COUNT(*) AS registros FROM workspace.silver.sales;
SELECT COUNT(*) AS registros FROM workspace.gold.fact_sales;

DESCRIBE DETAIL workspace.bronze.sales;
DESCRIBE DETAIL workspace.silver.sales;
DESCRIBE DETAIL workspace.gold.fact_sales;

-- Validación del CDF de Bronze
SELECT *
FROM table_changes('workspace.bronze.sales', 0)
LIMIT 20;

-- Validación del modelo estrella
SELECT COUNT(*) AS fact_rows FROM workspace.gold.fact_sales;
SELECT COUNT(*) AS dim_product_rows FROM workspace.gold.dim_products;
SELECT COUNT(*) AS dim_customer_rows FROM workspace.gold.dim_customers;
SELECT COUNT(*) AS dim_store_rows FROM workspace.gold.dim_stores;
SELECT COUNT(*) AS dim_calendar_rows FROM workspace.gold.dim_calendar;
