# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Módulo 3 · Capa Silver Incremental con Change Data Feed
# MAGIC
# MAGIC **Objetivo:** Consumir incrementalmente los cambios desde `bronze_uses` mediante **Delta Change Data Feed (CDF)**, aplicar limpieza, deduplicación por `id`, parseo robusto de fechas (`TIMESTAMP`), casteo de saldos (`DECIMAL(7,2)`) y consolidar mediante `MERGE` idempotente en `silver_uses`.
# MAGIC
# MAGIC ```text
# MAGIC <catalog>.trx_mifare.bronze_uses (CDF habilitado)
# MAGIC         │
# MAGIC         ▼  readStream (readChangeFeed=true + trigger availableNow)
# MAGIC Normalización PySpark (TIMESTAMP + DECIMAL(7,2) + Deduplicación)
# MAGIC         │
# MAGIC         ▼  MERGE INTO (idempotente por trx_id)
# MAGIC <catalog>.trx_mifare.silver_uses (Delta Table)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./000_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · Configuración de Tablas, Checkpoint y Habilitación de CDF en Bronze

# COMMAND ----------

import json
from pyspark.sql.window import Window

TABLA_BRONZE      = qualified_name(BRONZE_USES)
TABLA_SILVER      = qualified_name(SILVER_USES)
CHECKPOINT_SILVER = f"{BASE}/checkpoints/{SILVER_USES}"

# Asegurar que Bronze tenga Change Data Feed (CDF) habilitado
props = spark.sql(f"SHOW TBLPROPERTIES {TABLA_BRONZE}").collect()
cdf_activo = any(r["key"] == "delta.enableChangeDataFeed" and r["value"].lower() == "true" for r in props)

if not cdf_activo:
    spark.sql(f"ALTER TABLE {TABLA_BRONZE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"✔ Propiedad delta.enableChangeDataFeed = true habilitada en {TABLA_BRONZE}")
else:
    print(f"✔ CDF ya se encontraba activo en {TABLA_BRONZE}")

# Obtener versión inicial de CDF en caso de ser la primera ejecución de Silver
version_actual_bronze = spark.sql(f"DESCRIBE HISTORY {TABLA_BRONZE} LIMIT 1").first()["version"]

print(f"Tabla origen Bronze  : {TABLA_BRONZE} (versión actual: {version_actual_bronze})")
print(f"Tabla destino Silver : {TABLA_SILVER}")
print(f"Checkpoint Silver    : {CHECKPOINT_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 · Creación de Tabla Delta Silver (si no existe)
# MAGIC
# MAGIC Esquema alineado con los tipos de dato de Oracle (`SW_MIFARE_TRX`) y optimizado con `CLUSTER BY (fecha_trx)`.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLA_SILVER} (
        trx_id                  BIGINT          NOT NULL,
        creation_date           TIMESTAMP,
        trx_date                TIMESTAMP,
        fecha_trx               DATE,
        terminal_id             BIGINT,
        tsn                     BIGINT,
        bus_id                  BIGINT,
        line_id                 BIGINT,
        app_id                  BIGINT,
        use_type                BIGINT,
        fee                     BIGINT,
        uuid                    BIGINT,
        flee_control            STRING,
        state                   BIGINT,
        state_desc              STRING,
        project_id              INT,
        purseavalue             DECIMAL(7,2),
        pursebvalue             DECIMAL(7,2),
        cut_id                  SMALLINT,
        error_code              BIGINT,
        tp_id                   BIGINT,
        archivo_origen          STRING,
        bronze_ingestado_en     TIMESTAMP,
        silver_actualizado_en   TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (fecha_trx)
    TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 · Funciones de Transformación (Fechas `TIMESTAMP` y Saldos `DECIMAL(7,2)`)

# COMMAND ----------

def parsear_timestamp_seguro(col_name: str):
    """
    Convierte un STRING de fecha de la API a tipo TIMESTAMP nativo de Delta Lake
    usando F.coalesce + F.try_to_timestamp para tolerar formatos distintos en modo ANSI.
    """
    c = F.trim(F.col(col_name))
    return F.coalesce(
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS")),
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss.SSS")),
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(c, F.lit("dd-MM-yyyy HH:mm:ss")),
        F.try_to_timestamp(c, F.lit("dd/MM/yyyy HH:mm:ss")),
        F.try_to_timestamp(c)
    )


def transformar_lote_silver(df_cdf):
    """
    Filtra cambios válidos del CDF, deduplica por id dentro del micro-lote
    y aplica las reglas de normalización de Silver.
    """
    # Conservar únicamente inserciones y actualizaciones posteriores (ignorar delete / update_preimage)
    df_validos = df_cdf.filter(
        F.col("_change_type").isin("insert", "update_postimage") &
        F.col("id").isNotNull()
    )

    # Deduplicar por id dentro del micro-lote tomando la última versión de commit
    ventana_dedup = Window.partitionBy("id").orderBy(
        F.col("_commit_version").desc(),
        F.col("bronze_ingestado_en").desc()
    )

    df_dedup = (
        df_validos
        .withColumn("_rn", F.row_number().over(ventana_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Normalización de tipos y nombres alineados con Oracle (SW_MIFARE_TRX)
    return (
        df_dedup
        .withColumn("trx_date_ts", parsear_timestamp_seguro("date"))
        .withColumn("creation_date_ts", parsear_timestamp_seguro("fechaCreacion"))
        .select(
            F.col("id").cast("bigint").alias("trx_id"),
            F.col("creation_date_ts").alias("creation_date"),
            F.col("trx_date_ts").alias("trx_date"),
            F.to_date(F.col("trx_date_ts")).alias("fecha_trx"),
            F.col("validator_id").cast("bigint").alias("terminal_id"),
            F.col("tsn").cast("bigint").alias("tsn"),
            F.col("bus_id").cast("bigint").alias("bus_id"),
            F.col("line_id").cast("bigint").alias("line_id"),
            F.col("applic_id").cast("bigint").alias("app_id"),
            F.col("type_use").cast("bigint").alias("use_type"),
            F.col("fee").cast("bigint").alias("fee"),
            F.col("uid_target").cast("bigint").alias("uuid"),
            F.trim(F.col("flee_control")).alias("flee_control"),
            F.col("state").cast("bigint").alias("state"),
            F.trim(F.col("state_description")).alias("state_desc"),
            F.lit(API_PROJECT_ID).cast("int").alias("project_id"),
            F.expr("try_cast(purseavalue as decimal(7,2))").alias("purseavalue"),
            F.expr("try_cast(pursebvalue as decimal(7,2))").alias("pursebvalue"),
            F.expr("try_cast(cut_id as smallint)").alias("cut_id"),
            F.col("errorcode").cast("bigint").alias("error_code"),
            F.col("tp_Id").cast("bigint").alias("tp_id"),
            F.col("archivo_origen"),
            F.col("bronze_ingestado_en"),
            F.current_timestamp().alias("silver_actualizado_en")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 · Ejecución Incremental con `readChangeFeed` + `MERGE INTO`

# COMMAND ----------

def upsert_a_silver(micro_batch_df, batch_id: int):
    if micro_batch_df.isEmpty():
        print(f"Batch {batch_id}: sin cambios nuevos en CDF.")
        return

    df_silver_lote = transformar_lote_silver(micro_batch_df)
    df_silver_lote.createOrReplaceTempView("vw_silver_updates")

    micro_batch_df.sparkSession.sql(f"""
        MERGE INTO {TABLA_SILVER} AS target
        USING vw_silver_updates AS source
        ON target.trx_id = source.trx_id
        WHEN MATCHED THEN UPDATE SET
            target.creation_date         = source.creation_date,
            target.trx_date              = source.trx_date,
            target.fecha_trx             = source.fecha_trx,
            target.terminal_id           = source.terminal_id,
            target.tsn                   = source.tsn,
            target.bus_id                = source.bus_id,
            target.line_id               = source.line_id,
            target.app_id                = source.app_id,
            target.use_type              = source.use_type,
            target.fee                   = source.fee,
            target.uuid                  = source.uuid,
            target.flee_control          = source.flee_control,
            target.state                 = source.state,
            target.state_desc            = source.state_desc,
            target.project_id            = source.project_id,
            target.purseavalue           = source.purseavalue,
            target.pursebvalue           = source.pursebvalue,
            target.cut_id                = source.cut_id,
            target.error_code            = source.error_code,
            target.tp_id                 = source.tp_id,
            target.archivo_origen        = source.archivo_origen,
            target.bronze_ingestado_en   = source.bronze_ingestado_en,
            target.silver_actualizado_en = source.silver_actualizado_en
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✔ Batch {batch_id}: MERGE completado en {TABLA_SILVER}.")


# 1. Detectar automáticamente si existe checkpoint previo en Silver
checkpoint_silver_existe = False
try:
    checkpoint_silver_existe = len(dbutils.fs.ls(CHECKPOINT_SILVER)) > 0
except Exception:
    checkpoint_silver_existe = False

# Sincronizar lote inicial únicamente si no existe checkpoint (primera ejecución)
if not checkpoint_silver_existe:
    print("Primera ejecución detectada en Silver: cargando lote inicial (Batch 0)...")
    df_lote_inicial = (
        spark.table(TABLA_BRONZE)
        .withColumn("_change_type", F.lit("insert"))
        .withColumn("_commit_version", F.lit(0).cast("bigint"))
    )
    upsert_a_silver(df_lote_inicial, batch_id=0)

# 2. Continuar con lectura incremental mediante Change Data Feed (CDF)
lector_cdf = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
)

if not checkpoint_silver_existe:
    lector_cdf = lector_cdf.option("startingVersion", version_actual_bronze)

df_bronze_cdf = lector_cdf.table(TABLA_BRONZE)

query_silver = (
    df_bronze_cdf.writeStream
    .foreachBatch(upsert_a_silver)
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .start()
)

query_silver.awaitTermination()
print("✔ Procesamiento incremental CDF -> Silver finalizado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 · Auditoría de Calidad y Esquema de Silver

# COMMAND ----------

resumen_silver = spark.sql(f"""
    SELECT
        COUNT(*) AS total_registros_silver,
        COUNT(DISTINCT trx_id) AS trx_unicas,
        SUM(CASE WHEN trx_date IS NULL OR creation_date IS NULL THEN 1 ELSE 0 END) AS fechas_nulas,
        SUM(CASE WHEN purseavalue IS NULL OR pursebvalue IS NULL THEN 1 ELSE 0 END) AS saldos_nulos,
        MIN(trx_id) AS min_trx_id,
        MAX(trx_id) AS max_trx_id,
        MAX(silver_actualizado_en) AS ultima_actualizacion
    FROM {TABLA_SILVER}
""").first().asDict()

print("=" * 60)
print(f"RESUMEN CAPA SILVER ({TABLA_SILVER})")
print(f"  Total registros en Silver : {resumen_silver['total_registros_silver']:,}")
print(f"  Transacciones únicas      : {resumen_silver['trx_unicas']:,}")
print(f"  Fechas nulas (alerta)     : {resumen_silver['fechas_nulas']}")
print(f"  Saldos nulos (alerta)     : {resumen_silver['saldos_nulos']}")
print(f"  Rango de TRX_ID           : {resumen_silver['min_trx_id']} -> {resumen_silver['max_trx_id']}")
print(f"  Última actualización      : {resumen_silver['ultima_actualizacion']}")
print("=" * 60)

# COMMAND ----------

# Verificar tipos finales de la tabla Silver
spark.table(TABLA_SILVER).printSchema()

# COMMAND ----------

display(
    spark.table(TABLA_SILVER)
    .orderBy(F.col("trx_id").desc())
    .limit(20)
)

# COMMAND ----------

# Salida estructurada para orquestación en Databricks Workflows
dbutils.notebook.exit(json.dumps({
    "tabla": TABLA_SILVER,
    "total_registros": resumen_silver["total_registros_silver"],
    "max_trx_id": resumen_silver["max_trx_id"]
}))