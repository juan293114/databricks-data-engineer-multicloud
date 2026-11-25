# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 3 · Silver
# MAGIC bronze → silver con Change Data Feed.

# COMMAND ----------

# MAGIC %run ../00_Setup

# COMMAND ----------

build_silver()

minimo = 1
filas = spark.table(SILVER).count()
if filas < minimo:
    log_step("validacion", "ERROR", f"silver tiene {filas} filas")
    raise Exception("Silver quedó vacía: no se publica gold")
