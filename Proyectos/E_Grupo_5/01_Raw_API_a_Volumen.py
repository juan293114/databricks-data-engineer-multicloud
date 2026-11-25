# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · RAW: de la API Open-Meteo al Volumen
# MAGIC
# MAGIC **Fuente:** [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), pública, gratis y sin token.
# MAGIC Se pide el clima diario de 12 ciudades de Colombia y se guarda **cada respuesta tal cual** en el Volumen:
# MAGIC un archivo por ciudad y rango de fechas (`clima_<ciudad>_<desde>_<hasta>.json`).

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · Una llamada de prueba

# COMMAND ----------

r = requests.get("https://archive-api.open-meteo.com/v1/archive", params={
    "latitude": 4.7110, "longitude": -74.0721,                 # Bogotá
    "start_date": "2025-01-01", "end_date": "2025-01-03",
    "daily": ",".join(VARIABLES), "timezone": "America/Bogota",
}, timeout=60)
print("status:", r.status_code)
if r.status_code == 200:
    print(r.json()["daily"])
else:
    # 429 = límite de uso de la API. Databricks serverless comparte IPs de salida con muchos usuarios,
    # así que puede pasar aunque nosotros hagamos pocas llamadas. La descarga de abajo reintenta.
    print("La API no respondió bien:", r.text[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 · Carga histórica (parámetros)

# COMMAND ----------

dbutils.widgets.text("start_date", "2025-01-01", "Fecha inicial (AAAA-MM-DD)")
dbutils.widgets.text("end_date", (date.today() - timedelta(days=2)).isoformat(), "Fecha final (AAAA-MM-DD)")

desde = date.fromisoformat(dbutils.widgets.get("start_date"))
hasta = date.fromisoformat(dbutils.widgets.get("end_date"))
download_all(desde, hasta)

# COMMAND ----------

display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

print(dbutils.fs.head(f"{RAW_PATH}/{raw_files()[0]}", 600))
