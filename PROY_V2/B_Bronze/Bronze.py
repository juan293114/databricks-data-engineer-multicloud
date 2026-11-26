# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # B_Bronze - Auto Loader 
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import col, current_timestamp

CATALOG = "workspace"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

RAW_BASE = "/Volumes/workspace/default/filestore/tables/raw"
CHECKPOINT_BASE = "/Volumes/workspace/default/filestore/checkpoints/bronze"
SCHEMA_BASE = "/Volumes/workspace/default/filestore/schemas/bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")


# ============================================================
# CONFIGURACIÓN DE TABLAS
# ============================================================

TABLES = {

    "calendar": {
        "file_pattern": "calendar*.csv",
        "table": f"{BRONZE_SCHEMA}.calendar",
        "checkpoint": f"{CHECKPOINT_BASE}/calendar/",
        "schema": f"{SCHEMA_BASE}/calendar/",
    },

    "products": {
        "file_pattern": "products*.csv",
        "table": f"{BRONZE_SCHEMA}.products",
        "checkpoint": f"{CHECKPOINT_BASE}/products/",
        "schema": f"{SCHEMA_BASE}/products/",
    },

    "customers": {
        "file_pattern": "customers*.csv",
        "table": f"{BRONZE_SCHEMA}.customers",
        "checkpoint": f"{CHECKPOINT_BASE}/customers/",
        "schema": f"{SCHEMA_BASE}/customers/",
    },

    "stores": {
        "file_pattern": "stores*.csv",
        "table": f"{BRONZE_SCHEMA}.stores",
        "checkpoint": f"{CHECKPOINT_BASE}/stores/",
        "schema": f"{SCHEMA_BASE}/stores/",
    },

    "sales": {
        "file_pattern": "sales*.csv",
        "table": f"{BRONZE_SCHEMA}.sales",
        "checkpoint": f"{CHECKPOINT_BASE}/sales/",
        "schema": f"{SCHEMA_BASE}/sales/",
    },
}


# ============================================================
# FUNCIÓN AUTO LOADER
# ============================================================

def ingest_with_autoloader(cfg):

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            cfg["schema"]
        )
        .option(
            "pathGlobFilter",
            cfg["file_pattern"]
        )
        .option("header", "true")
        .load(RAW_BASE)
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_file",
            col("_metadata.file_path")
        )
    )

    query = (
        df.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            cfg["checkpoint"]
        )
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(cfg["table"])
    )

    query.awaitTermination()


# ============================================================
# EJECUCIÓN
# ============================================================

for name, cfg in TABLES.items():

    print(f"Iniciando Bronze: {name}")

    ingest_with_autoloader(cfg)

    print(
        f"Bronze OK: {cfg['table']}"
    )


print(
    "BRONZE - Auto Loader finalizado correctamente"
)




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'calendar' AS tabla, COUNT(*) AS total_filas FROM workspace.bronze.calendar
# MAGIC UNION ALL
# MAGIC SELECT 'products' AS tabla, COUNT(*) AS total_filas FROM workspace.bronze.products
# MAGIC UNION ALL
# MAGIC SELECT 'customers' AS tabla, COUNT(*) AS total_filas FROM workspace.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'stores' AS tabla, COUNT(*) AS total_filas FROM workspace.bronze.stores
# MAGIC UNION ALL
# MAGIC SELECT 'sales' AS tabla, COUNT(*) AS total_filas FROM workspace.bronze.sales;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.bronze.customers where customer_id='C000001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_file,
# MAGIC     COUNT(*) AS cantidad
# MAGIC FROM workspace.bronze.customers
# MAGIC GROUP BY source_file
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.bronze.customers
# MAGIC WHERE source_file LIKE '%customers_new.csv';