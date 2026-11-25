# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 1 · Ingesta a RAW
# MAGIC Descarga de la API los últimos días para las 12 ciudades. Parámetro del Job: `dias_atras`.

# COMMAND ----------

# MAGIC %run ../00_Setup

# COMMAND ----------

dbutils.widgets.text("dias_atras", "5")
dias_atras = int(dbutils.widgets.get("dias_atras"))

hasta = date.today() - timedelta(days=1)
desde = hasta - timedelta(days=dias_atras - 1)
print(f"Parámetros: dias_atras = {dias_atras} → {desde} a {hasta}")
try:
    res = download_all(desde, hasta)
    if res["falló"]:
        log_step("raw", "AVISO", f"{desde} a {hasta}: la API rechazó {res['falló']} descargas (429); se piden en la próxima corrida")
    else:
        log_step("raw", "OK", f"{desde} a {hasta}")
except Exception as e:
    log_step("raw", "ERROR", str(e)[:200])
    raise
