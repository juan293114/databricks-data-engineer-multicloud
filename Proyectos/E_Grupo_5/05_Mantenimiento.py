# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Mantenimiento: OPTIMIZE + ZORDER BY + VACUUM
# MAGIC
# MAGIC - **OPTIMIZE**: junta muchos archivos pequeños (cada MERGE diario crea algunos) en pocos archivos grandes.
# MAGIC - **ZORDER BY**: dentro de esos archivos ordena las filas por las columnas más filtradas
# MAGIC   (ciudad y fecha), así una consulta de "Barranquilla en marzo" lee menos archivos.
# MAGIC - **VACUUM**: borra del almacenamiento los archivos que ya no usa ninguna versión reciente.
# MAGIC   Se dejan **7 días (168 horas)** para no perder el time travel de la última semana.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL fact_clima_diario   -- mirar numFiles antes

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM fact_clima_diario DRY RUN    -- qué borraría, sin borrar nada

# COMMAND ----------

run_maintenance()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL fact_clima_diario   -- numFiles después

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pipeline_log ORDER BY registrado_en DESC
