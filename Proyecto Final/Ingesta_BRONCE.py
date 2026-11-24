# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Módulo 2 · Capa Bronze con Auto Loader
# MAGIC
# MAGIC **Objetivo:** Ingestar de forma incremental los archivos JSON almacenados en la capa RAW hacia la tabla Delta `bronze_uses` utilizando **Databricks Auto Loader (`cloudFiles`)**, explotando el arreglo `data.transactions` y preservando trazabilidad completa sin alterar los valores crudos.
# MAGIC
# MAGIC ```text
# MAGIC /Volumes/<catalog>/trx_mifare/raw_trx_mifare/uses/AAAA/MM/DD/*.json
# MAGIC         │
# MAGIC         ▼  Auto Loader (cloudFiles + trigger availableNow)
# MAGIC <catalog>.trx_mifare.bronze_uses (Delta Table)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./000_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · Configuración de Rutas de Checkpoint y Tabla Destino

# COMMAND ----------

import json


TABLA_BRONZE      = qualified_name(BRONZE_USES)
CHECKPOINT_BRONZE = f"{BASE}/checkpoints/{BRONZE_USES}"
SCHEMA_LOC_BRONZE = f"{BASE}/schemas/{BRONZE_USES}"

print(f"Ruta origen RAW    : {RAW_PATH}")
print(f"Tabla Delta Bronze : {TABLA_BRONZE}")
print(f"Checkpoint         : {CHECKPOINT_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 · Lectura Incremental con Auto Loader (`cloudFiles`)
# MAGIC
# MAGIC - **`cloudFiles.format = json` + `multiLine = True`**: Lee la estructura completa del JSON de Trafpay.
# MAGIC - **`pathGlobFilter = uses_*.json`**: Filtra exclusivamente los archivos de transacciones ignorando carpetas o archivos temporales.
# MAGIC - **`F.explode("data.transactions")`**: Convierte cada transacción del arreglo JSON en un registro individual en Bronze.

# COMMAND ----------

df_raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC_BRONZE)
    .option("multiLine", True)
    .option("pathGlobFilter", "uses_*.json")
    .schema(ESQUEMA_USES)
    .load(RAW_PATH)
)

df_bronze_stream = (
    df_raw_stream
    .filter(F.col("code") == 0)
    .withColumn("tx", F.explode(F.col("data.transactions")))
    .select(
        F.col("tx.*"),
        F.col("code").alias("api_code"),
        F.col("message").alias("api_message"),
        F.col("data.transactionsCount").alias("batch_transactions_count"),
        F.col("_metadata.file_path").alias("archivo_origen"),
        F.col("_metadata.file_modification_time").alias("archivo_modificado_en"),
        F.current_timestamp().alias("bronze_ingestado_en")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 · Escritura Incremental en Delta Lake (`availableNow=True`)
# MAGIC
# MAGIC Ejecuta el procesamiento por lotes incrementales hasta vaciar la cola de archivos nuevos en RAW y finaliza automáticamente.

# COMMAND ----------

query_bronze = (
    df_bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABLA_BRONZE)
)

query_bronze.awaitTermination()

# Habilitar Change Data Feed (CDF) sobre la tabla Bronze
spark.sql(f"ALTER TABLE {TABLA_BRONZE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"✔ Carga incremental con Auto Loader completada en {TABLA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 · Auditoría y Verificación de la Tabla Delta Bronze

# COMMAND ----------

progreso = query_bronze.lastProgress
filas_nuevas = progreso["numInputRows"] if progreso and "numInputRows" in progreso else 0

resumen_bronze = spark.sql(f"""
    SELECT
        COUNT(*) AS total_registros_bronze,
        COUNT(DISTINCT id) AS ids_unicos,
        MIN(id) AS min_id,
        MAX(id) AS max_id,
        MAX(bronze_ingestado_en) AS ultima_actualizacion
    FROM {TABLA_BRONZE}
""").first().asDict()

print("=" * 60)
print(f"RESUMEN CAPA BRONZE ({TABLA_BRONZE})")
print(f"  Archivos nuevos procesados : {filas_nuevas}")
print(f"  Total registros en Bronze  : {resumen_bronze['total_registros_bronze']:,}")
print(f"  IDs únicos en Bronze       : {resumen_bronze['ids_unicos']:,}")
print(f"  Rango de IDs               : {resumen_bronze['min_id']} -> {resumen_bronze['max_id']}")
print(f"  Última actualización       : {resumen_bronze['ultima_actualizacion']}")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(TABLA_BRONZE)
    .orderBy(F.col("id").desc())
    .limit(20)
)

# COMMAND ----------

# Salida estructurada para orquestación en Databricks Workflows
dbutils.notebook.exit(json.dumps({
    "tabla": TABLA_BRONZE,
    "total_registros": resumen_bronze["total_registros_bronze"],
    "max_id": resumen_bronze["max_id"]
}))