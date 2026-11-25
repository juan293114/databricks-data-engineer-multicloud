# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 4 · Gold
# MAGIC silver → modelo estrella (dim_ciudad, dim_fecha, fact_clima_diario).

# COMMAND ----------

# MAGIC %run ../00_Setup

# COMMAND ----------

build_gold()
display(spark.table(FACT).orderBy(F.col("fecha_key").desc()).limit(5))
