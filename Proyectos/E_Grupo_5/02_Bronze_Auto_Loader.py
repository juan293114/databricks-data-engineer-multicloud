# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · BRONZE con Auto Loader
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) detecta los archivos **nuevos** de RAW y los carga en `bronze_clima` (Delta).
# MAGIC El checkpoint recuerda qué archivos ya leyó: si se corre dos veces, la segunda no hace nada.
# MAGIC La tabla tiene **Change Data Feed** activado para que silver lea solo los cambios.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

create_bronze_table()
display(spark.sql(f"SHOW TBLPROPERTIES {BRONZE}").where("key = 'delta.enableChangeDataFeed'"))

# COMMAND ----------

lectura = autoloader_read()
print("¿Es un stream?", lectura.isStreaming)
lectura.printSchema()

# COMMAND ----------

run_bronze()
print("Filas en bronze (una por archivo):", spark.table(BRONZE).count())

# COMMAND ----------

display(spark.table(BRONZE).select("source_file", "latitude", "longitude", "elevation",
                                   F.size("daily.time").alias("dias"), "ingested_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segunda corrida: no hay archivos nuevos → no pasa nada

# COMMAND ----------

antes = table_version(BRONZE)
run_bronze()
print("Versión antes:", antes, "| después:", table_version(BRONZE))

# COMMAND ----------

display(spark.sql(f"SELECT path, commit_time FROM cloud_files_state('{CHECKPOINT_PATH}') ORDER BY commit_time DESC"))

# COMMAND ----------

show_history(BRONZE)
