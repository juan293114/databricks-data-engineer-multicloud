# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · SILVER con Change Data Feed
# MAGIC
# MAGIC Silver lee de bronze **solo los cambios nuevos** (CDF) desde la última versión procesada
# MAGIC (guardada en `pipeline_state`) y los aplica con `MERGE`.
# MAGIC Resultado: una fila por **ciudad y día**, limpia y con tipos correctos.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lo que registró el Change Data Feed de bronze
# MAGIC SELECT _commit_version, _change_type, count(*) AS archivos
# MAGIC FROM table_changes('bronze_clima', 0)
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1

# COMMAND ----------

# Antes de cargar: así queda un archivo de bronze aplanado
display(to_silver(spark.table(BRONZE)).limit(10))

# COMMAND ----------

build_silver()
print("Filas en silver:", spark.table(SILVER).count())
display(spark.table(STATE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segunda corrida: silver ya está al día

# COMMAND ----------

build_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ciudad_codigo, count(*) AS dias, min(fecha) AS desde, max(fecha) AS hasta,
# MAGIC        round(avg(temp_media_c), 1) AS temp_media, round(sum(precipitacion_mm)) AS lluvia_total_mm
# MAGIC FROM silver_clima_diario
# MAGIC GROUP BY ciudad_codigo
# MAGIC ORDER BY ciudad_codigo

# COMMAND ----------

show_history(SILVER)
