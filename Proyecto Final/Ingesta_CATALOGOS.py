# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Módulo · Ingesta de Catálogos Maestros (RAW Excel → Bronze → Silver)
# MAGIC
# MAGIC **Objetivo:** Leer los archivos `.xlsx` depositados en `/Volumes/<catalog>/trx_mifare/raw_trx_mifare/catalogos/`, persistirlos sin alteraciones en tablas **Bronze** con metadatos de auditoría y consolidarlos limpios y deduplicados en **Silver** listos para cruces en Gold.

# COMMAND ----------

# Instalar openpyxl si no está disponible en el entorno Serverless
%pip install -q openpyxl

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./000_Setup

# COMMAND ----------

import json
import re
from pathlib import Path
import pandas as pd

dbutils.fs.mkdirs(CATALOGOS_PATH)

CATALOGOS_CONFIG = [
    {
        "nombre": "applications",
        "prefijo": "aplications_",
        "tabla_bronze": qualified_name(BRONZE_APPLICATIONS),
        "tabla_silver": qualified_name(SILVER_APPLICATIONS),
    },
    {
        "nombre": "lines",
        "prefijo": "line_",
        "tabla_bronze": qualified_name(BRONZE_LINES),
        "tabla_silver": qualified_name(SILVER_LINES),
    },
    {
        "nombre": "providers",
        "prefijo": "providers_",
        "tabla_bronze": qualified_name(BRONZE_PROVIDERS),
        "tabla_silver": qualified_name(SILVER_PROVIDERS),
    },
]

def limpiar_nombre_columna(col: str) -> str:
    """Normaliza encabezados de Excel a snake_case válido para Delta Lake."""
    limpio = re.sub(r"[^0-9a-zA-Z_]+", "_", str(col).strip().lower())
    return limpio.strip("_") or "col_sin_nombre"

def obtener_excel_mas_reciente(carpeta: str, prefijo: str) -> Path:
    """Busca el archivo .xlsx más reciente que inicie con el prefijo indicado."""
    ruta_dir = Path(carpeta)
    coincidencias = sorted(
        [f for f in ruta_dir.glob(f"{prefijo}*.xlsx") if not f.name.startswith("~$")],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if not coincidencias:
        raise FileNotFoundError(f"No se encontró ningún archivo '{prefijo}*.xlsx' en {carpeta}")
    return coincidencias[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · Persistencia en Capas Bronze y Silver

# COMMAND ----------

resumen_catalogos = []

for cfg in CATALOGOS_CONFIG:
    archivo_xlsx = obtener_excel_mas_reciente(CATALOGOS_PATH, cfg["prefijo"])
    print(f"\n⏳ Procesando catálogo '{cfg['nombre']}' desde: {archivo_xlsx.name}")

    # 1. Leer Excel con pandas (todo como texto inicialmente para evitar pérdida de ceros o errores de tipo en Bronze)
    pdf = pd.read_excel(archivo_xlsx, sheet_name=0, dtype=str)
    pdf = pdf.dropna(how="all")
    pdf.columns = [limpiar_nombre_columna(c) for c in pdf.columns]

    # Convertir a Spark DataFrame
    df_raw = spark.createDataFrame(pdf)

    # 2. Capa BRONZE: datos crudos + trazabilidad del archivo origen
    df_bronze = (
        df_raw
        .withColumn("archivo_origen", F.lit(str(archivo_xlsx)))
        .withColumn("bronze_ingestado_en", F.current_timestamp())
    )

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cfg["tabla_bronze"])
    )
    spark.sql(f"ALTER TABLE {cfg['tabla_bronze']} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    # 3. Capa SILVER: limpieza de espacios, casteando la primera columna (ID) a BIGINT si es numérica
    cols_datos = [c for c in df_raw.columns]
    col_id = cols_datos[0]  # Primera columna del catálogo (ej. id / applic_id / line_id / provider_id)

    df_silver = (
        df_raw
        .select([
            F.expr(f"try_cast(trim(`{c}`) as bigint)").alias(c) if c == col_id
            else F.trim(F.col(f"`{c}`")).alias(c)
            for c in cols_datos
        ])
        .filter(F.col(col_id).isNotNull())
        .dropDuplicates([col_id])
        .withColumn("archivo_origen", F.lit(str(archivo_xlsx)))
        .withColumn("silver_actualizado_en", F.current_timestamp())
    )

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cfg["tabla_silver"])
    )
    spark.sql(f"ALTER TABLE {cfg['tabla_silver']} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    total_filas = df_silver.count()
    resumen_catalogos.append({
        "catalogo": cfg["nombre"],
        "archivo": archivo_xlsx.name,
        "bronze": cfg["tabla_bronze"],
        "silver": cfg["tabla_silver"],
        "registros": total_filas
    })
    print(f"✔ {cfg['tabla_bronze']} y {cfg['tabla_silver']} actualizadas ({total_filas:,} registros).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 · Verificación de Esquemas y Registros en Silver

# COMMAND ----------

for item in resumen_catalogos:
    print("=" * 60)
    print(f"Catálogo : {item['catalogo']} ({item['archivo']}) -> {item['registros']:,} filas")
    spark.table(item["silver"]).printSchema()
    display(spark.table(item["silver"]).limit(10))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(resumen_catalogos))