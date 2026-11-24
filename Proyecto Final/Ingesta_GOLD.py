# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Módulo 4 · Capa Gold Incremental con Change Data Feed

# COMMAND ----------

# MAGIC %run ./000_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · Configuración de Tablas y Checkpoint Gold

# COMMAND ----------

import json

TABLA_SILVER_USES   = qualified_name(SILVER_USES)
TABLA_SILVER_LINES  = qualified_name(SILVER_LINES)
TABLA_SILVER_APPS   = qualified_name(SILVER_APPLICATIONS)
TABLA_SILVER_PROV   = qualified_name(SILVER_PROVIDERS)

TABLA_GOLD_USES     = qualified_name(GOLD_USES_TX)
TABLA_GOLD_LINES    = qualified_name(GOLD_LINES)
TABLA_GOLD_APPS     = qualified_name(GOLD_APPLICATIONS)
TABLA_GOLD_PROV     = qualified_name(GOLD_PROVIDERS)

CHECKPOINT_GOLD     = f"{BASE}/checkpoints/{GOLD_USES_TX}"

# Validar que silver_uses exista antes de consultar propiedades o historial
if not spark.catalog.tableExists(TABLA_SILVER_USES):
    dbutils.notebook.exit(json.dumps({
        "estado": "omitido",
        "motivo": f"La tabla origen {TABLA_SILVER_USES} aún no existe. Ejecuta Ingesta_SILVER primero."
    }))

# Verificar CDF activo en silver_uses
props = spark.sql(f"SHOW TBLPROPERTIES {TABLA_SILVER_USES}").collect()
cdf_activo = any(r["key"] == "delta.enableChangeDataFeed" and r["value"].lower() == "true" for r in props)
if not cdf_activo:
    spark.sql(f"ALTER TABLE {TABLA_SILVER_USES} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

version_actual_silver = spark.sql(f"DESCRIBE HISTORY {TABLA_SILVER_USES} LIMIT 1").first()["version"]

print(f"Origen Silver Uses : {TABLA_SILVER_USES} (versión actual: {version_actual_silver})")
print(f"Destino Gold Uses  : {TABLA_GOLD_USES}")
print(f"Checkpoint Gold    : {CHECKPOINT_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 · Persistencia de las 3 Tablas de Catálogo en Gold y Sincronización Retroactiva

# COMMAND ----------

catalogos_gold = [
    (TABLA_SILVER_LINES, TABLA_GOLD_LINES, "ld_id"),
    (TABLA_SILVER_APPS,  TABLA_GOLD_APPS,  "app_id"),
    (TABLA_SILVER_PROV,  TABLA_GOLD_PROV,  "tp_id"),
]

for origen_silver, destino_gold, llave in catalogos_gold:
    df_cat = (
        spark.table(origen_silver)
        .drop("archivo_origen", "silver_actualizado_en")
        .withColumn("gold_actualizado_en", F.current_timestamp())
    )
    (
        df_cat.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(destino_gold)
    )
    print(f"✔ Catálogo Gold actualizado: {destino_gold} ({df_cat.count():,} registros, llave: {llave})")

# Sincronización retroactiva: si un Excel de catálogo actualizó un nombre, corrige registros históricos en Gold
if spark.catalog.tableExists(TABLA_GOLD_USES):
    spark.sql(f"""
        MERGE INTO {TABLA_GOLD_USES} AS target
        USING (
            SELECT
                g.Fecha_trx,
                g.hora_trx,
                g.linea,
                g.empresa,
                g.aplicacion,
                g.tipo_uso,
                COALESCE(b.ld_id_name, CONCAT('Sin catálogo Línea (', CAST(g.linea AS STRING), ')'))     AS nuevo_linea_nombre,
                COALESCE(d.tp_id_name, CONCAT('Sin catálogo Empresa (', CAST(g.empresa AS STRING), ')')) AS nuevo_empresa_nombre,
                COALESCE(c.app_name,   CONCAT('Sin catálogo App (', CAST(g.aplicacion AS STRING), ')'))  AS nuevo_aplicacion_nombre
            FROM {TABLA_GOLD_USES} g
            LEFT JOIN {TABLA_GOLD_LINES} b ON b.ld_id  = g.linea
            LEFT JOIN {TABLA_GOLD_PROV}  d ON d.tp_id  = g.empresa
            LEFT JOIN {TABLA_GOLD_APPS}  c ON c.app_id = g.aplicacion
        ) AS src
        ON  target.Fecha_trx  <=> src.Fecha_trx
        AND target.hora_trx   <=> src.hora_trx
        AND target.linea      <=> src.linea
        AND target.empresa    <=> src.empresa
        AND target.aplicacion <=> src.aplicacion
        AND target.tipo_uso   <=> src.tipo_uso
        WHEN MATCHED AND (
            target.linea_nombre      <> src.nuevo_linea_nombre OR
            target.empresa_nombre    <> src.nuevo_empresa_nombre OR
            target.aplicacion_nombre <> src.nuevo_aplicacion_nombre
        ) THEN UPDATE SET
            target.linea_nombre        = src.nuevo_linea_nombre,
            target.empresa_nombre      = src.nuevo_empresa_nombre,
            target.aplicacion_nombre   = src.nuevo_aplicacion_nombre,
            target.gold_actualizado_en = current_timestamp()
    """)
    print("✔ Sincronización retroactiva de catálogos verificada en Gold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 · Creación de Tabla Agregada `gold_uses_tx` (si no existe)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLA_GOLD_USES} (
        Fecha_trx           STRING,
        ano_trx             STRING,
        mes_trx             STRING,
        dia_trx             STRING,
        hora_trx            STRING,
        linea               BIGINT,
        linea_nombre        STRING,
        empresa             BIGINT,
        empresa_nombre      STRING,
        aplicacion          BIGINT,
        aplicacion_nombre   STRING,
        tipo_uso            BIGINT,
        total_trx           BIGINT,
        cant_trx            BIGINT,
        gold_actualizado_en TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (Fecha_trx, hora_trx)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 · Procesamiento Incremental Blindado (`silver_uses` CDF → `gold_uses_tx`)

# COMMAND ----------

def recalcular_ventanas_gold(df_ventanas_afectadas, batch_id: int):
    if df_ventanas_afectadas.isEmpty():
        print(f"Batch {batch_id}: sin cambios nuevos en Silver CDF.")
        return

    df_ventanas_afectadas.createOrReplaceTempView("vw_ventanas_afectadas")

    # 1. Recalcular agregaciones filtrando por fecha_trx (DATE) y usando LEFT JOIN + COALESCE
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW vw_gold_recalculado AS
        SELECT
            date_format(a.trx_date, 'yyyy-MM-dd')                                                                     AS Fecha_trx,
            date_format(a.trx_date, 'yyyy')                                                                           AS ano_trx,
            date_format(a.trx_date, 'MM')                                                                             AS mes_trx,
            date_format(a.trx_date, 'dd')                                                                             AS dia_trx,
            date_format(a.trx_date, 'HH')                                                                             AS hora_trx,
            COALESCE(a.line_id, -1L)                                                                                  AS linea,
            COALESCE(b.ld_id_name, CONCAT('Sin catálogo Línea (', COALESCE(CAST(a.line_id AS STRING), 'NULL'), ')'))  AS linea_nombre,
            COALESCE(a.tp_id, -1L)                                                                                    AS empresa,
            COALESCE(d.tp_id_name, CONCAT('Sin catálogo Empresa (', COALESCE(CAST(a.tp_id AS STRING), 'NULL'), ')'))  AS empresa_nombre,
            COALESCE(a.app_id, -1L)                                                                                   AS aplicacion,
            COALESCE(c.app_name,   CONCAT('Sin catálogo App (', COALESCE(CAST(a.app_id AS STRING), 'NULL'), ')'))     AS aplicacion_nombre,
            COALESCE(a.use_type, -1L)                                                                                 AS tipo_uso,
            SUM(COALESCE(a.fee, 0L) * 10L)                                                                            AS total_trx,
            COUNT(*)                                                                                                  AS cant_trx,
            current_timestamp()                                                                                       AS gold_actualizado_en
        FROM {TABLA_SILVER_USES} a
        INNER JOIN vw_ventanas_afectadas v
            ON  a.fecha_trx = v.fecha_trx_date
            AND date_format(a.trx_date, 'HH') = v.hora_trx
        LEFT JOIN {TABLA_GOLD_LINES} b
            ON b.ld_id = a.line_id
        LEFT JOIN {TABLA_GOLD_APPS} c
            ON c.app_id = a.app_id
        LEFT JOIN {TABLA_GOLD_PROV} d
            ON d.tp_id = a.tp_id
        GROUP BY ALL
    """)

    # 2. Eliminar ventanas afectadas previas para evitar filas fantasma
    spark.sql(f"""
        DELETE FROM {TABLA_GOLD_USES} AS target
        WHERE EXISTS (
            SELECT 1
            FROM vw_ventanas_afectadas v
            WHERE target.Fecha_trx = v.Fecha_trx
              AND target.hora_trx  = v.hora_trx
        )
    """)

    # 3. Insertar el recálculo limpio y exacto
    spark.sql(f"""
        INSERT INTO {TABLA_GOLD_USES}
        SELECT * FROM vw_gold_recalculado
    """)

    print(f"✔ Batch {batch_id}: Ventanas recalculadas (DELETE + INSERT) en {TABLA_GOLD_USES}.")


def procesar_micro_lote_cdf(micro_batch_df, batch_id: int):
    if micro_batch_df.isEmpty():
        print(f"Batch {batch_id}: sin cambios nuevos en Silver CDF.")
        return

    df_ventanas = (
        micro_batch_df
        .filter(F.col("_change_type").isin("insert", "update_postimage") & F.col("trx_date").isNotNull())
        .select(
            F.col("fecha_trx").alias("fecha_trx_date"),
            F.date_format("trx_date", "yyyy-MM-dd").alias("Fecha_trx"),
            F.date_format("trx_date", "HH").alias("hora_trx")
        )
        .distinct()
    )
    recalcular_ventanas_gold(df_ventanas, batch_id)


# Detección automática de primera ejecución (Batch 0)
checkpoint_gold_existe = False
try:
    checkpoint_gold_existe = len(dbutils.fs.ls(CHECKPOINT_GOLD)) > 0
except Exception:
    checkpoint_gold_existe = False

if not checkpoint_gold_existe:
    print("Primera ejecución detectada en Gold: ejecutando carga histórica inicial (Batch 0)...")
    df_ventanas_iniciales = (
        spark.table(TABLA_SILVER_USES)
        .filter(F.col("trx_date").isNotNull())
        .select(
            F.col("fecha_trx").alias("fecha_trx_date"),
            F.date_format("trx_date", "yyyy-MM-dd").alias("Fecha_trx"),
            F.date_format("trx_date", "HH").alias("hora_trx")
        )
        .distinct()
    )
    recalcular_ventanas_gold(df_ventanas_iniciales, batch_id=0)

# Lectura incremental con Change Data Feed (CDF)
lector_silver_cdf = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
)

if not checkpoint_gold_existe:
    lector_silver_cdf = lector_silver_cdf.option("startingVersion", version_actual_silver)

df_silver_cdf = lector_silver_cdf.table(TABLA_SILVER_USES)

query_gold = (
    df_silver_cdf.writeStream
    .foreachBatch(procesar_micro_lote_cdf)
    .option("checkpointLocation", CHECKPOINT_GOLD)
    .trigger(availableNow=True)
    .start()
)

query_gold.awaitTermination()
print("Procesamiento incremental Silver CDF -> Gold finalizado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 · Auditoría y Verificación de la Capa Gold

# COMMAND ----------

resumen_gold = spark.sql(f"""
    SELECT
        COUNT(*) AS filas_agregadas_gold,
        SUM(cant_trx) AS total_transacciones,
        SUM(total_trx) AS monto_total,
        MIN(Fecha_trx) AS fecha_min,
        MAX(Fecha_trx) AS fecha_max,
        MAX(gold_actualizado_en) AS ultima_actualizacion
    FROM {TABLA_GOLD_USES}
""").first().asDict()

print("=" * 60)
print(f"RESUMEN CAPA GOLD ({TABLA_GOLD_USES})")
print(f"  Filas agregadas en Gold   : {resumen_gold['filas_agregadas_gold']:,}")
print(f"  Total transacciones (sum) : {resumen_gold['total_transacciones']}")
print(f"  Monto total (fee * 10)    : {resumen_gold['monto_total']}")
print(f"  Rango de fechas           : {resumen_gold['fecha_min']} -> {resumen_gold['fecha_max']}")
print(f"  Última actualización      : {resumen_gold['ultima_actualizacion']}")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(TABLA_GOLD_USES)
    .orderBy(F.col("Fecha_trx").asc(), F.col("hora_trx").asc())
    .limit(50)
)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "tabla_uses": TABLA_GOLD_USES,
    "catalogos_gold": [TABLA_GOLD_LINES, TABLA_GOLD_APPS, TABLA_GOLD_PROV],
    "filas_agregadas": resumen_gold["filas_agregadas_gold"],
    "total_transacciones": resumen_gold["total_transacciones"]
}))