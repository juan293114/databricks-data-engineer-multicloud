# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # A_Raw - Extracción / validación de archivos fuente
# MAGIC
# MAGIC Esta capa conserva los archivos fuente en su formato original dentro de Volumes.
# MAGIC No transforma ni normaliza los datos. Las capas posteriores consumen estos archivos.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

RAW_BASE = "/Volumes/workspace/default/filestore/tables/raw"

SOURCES = {
    "calendar": f"{RAW_BASE}/calendar.csv",
    "products": f"{RAW_BASE}/products.csv",
    "customers": f"{RAW_BASE}/customers.csv",
    "stores": f"{RAW_BASE}/stores.csv",
    "sales": f"{RAW_BASE}/sales.csv",
}


def read_raw_csv(path):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


df_calendar_raw = read_raw_csv(SOURCES["calendar"])
df_products_raw = read_raw_csv(SOURCES["products"])
df_customers_raw = read_raw_csv(SOURCES["customers"])
df_stores_raw = read_raw_csv(SOURCES["stores"])
df_sales_raw = read_raw_csv(SOURCES["sales"])

print("RAW - archivos validados correctamente")
for name, path in SOURCES.items():
    print(f"{name}: {path}")


# COMMAND ----------

display(df_calendar_raw)