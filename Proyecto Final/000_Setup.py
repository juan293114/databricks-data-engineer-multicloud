# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 · Setup compartido Proyecto

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ubicación: Catálogo, Esquema y Volumen

# COMMAND ----------

import time
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F

ZONA_HORARIA = ZoneInfo("America/Bogota")

CATALOG = spark.sql("SELECT current_catalog()").first()[0]   # 'workspace' o 'main'
SCHEMA  = "trx_mifare"                                       # Esquema único para Bronze, Silver y Gold
VOLUME  = "raw_trx_mifare"                                   # Volumen para almacenamiento de archivos RAW

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rutas, Endpoints y Nombres de Tablas

# COMMAND ----------

# Archivos (Unity Catalog Volume)
BASE            = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_PATH        = f"{BASE}/uses"                          # Capa RAW: JSON sin modificar dentro de raw_trx_mifare
RAW_GLOB        = f"{RAW_PATH}/*/*/*/uses_*.json"         # Patrón: AAAA/MM/DD/uses_AAAAMMDD_HHMM_loteXXX.json
CATALOGOS_PATH  = f"{BASE}/catalogos"                     # Capa RAW: Archivos Excel de catálogos maestros
TMP_PATH        = f"{BASE}/tmp"
CHECKPOINT_PATH = f"{BASE}/checkpoints"

# Tablas Delta (Capas Bronze, Silver y Gold dentro de trx_mifare)
BRONZE_USES   = "bronze_uses"
SILVER_USES   = "silver_uses"
GOLD_USES_TX  = "gold_uses_tx"

# Tablas de Catálogos (Bronze, Silver y Gold)
BRONZE_APPLICATIONS = "bronze_applications"
SILVER_APPLICATIONS = "silver_applications"
GOLD_APPLICATIONS   = "gold_applications"

BRONZE_LINES        = "bronze_lines"
SILVER_LINES        = "silver_lines"
GOLD_LINES          = "gold_lines"

BRONZE_PROVIDERS    = "bronze_providers"
SILVER_PROVIDERS    = "silver_providers"
GOLD_PROVIDERS      = "gold_providers"



# Configuración API Trafpay
API_ENDPOINT_DEFAULT = "https://api.trafpay.cl/api-mifare/transactions/GetTransacionByID"
API_PROJECT_ID       = 1
API_DEFAULT_TRX_ID   = 164222894                        # ID inicial de respaldo si Silver aún está vacía
API_SALTO_SONDA      = 15000                            # Salto de IDs para evitar respuestas > 15k registros (evita 502)
API_MAX_LOTES        = 30                               # Tope de lotes por ejecución (~450k registros)
API_TIMEOUT_DEFAULT  = 300
SECRET_SCOPE         = "key-apis"
SECRET_KEY           = "usosapikey"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Esquema explícito del JSON de Trafpay
# MAGIC
# MAGIC Basado en la estructura `data.transactions` procesada en la capa Bronze.

# COMMAND ----------

ESQUEMA_USES = """
    code BIGINT,
    message STRING,
    data STRUCT<
        transactionsCount: BIGINT,
        transactions: ARRAY<STRUCT<
            applic_id: BIGINT,
            bus_id: BIGINT,
            cut_id: BIGINT,
            date: STRING,
            errorcode: BIGINT,
            fechaCreacion: STRING,
            fee: BIGINT,
            flee_control: STRING,
            id: BIGINT,
            line_id: BIGINT,
            purseavalue: BIGINT,
            pursebvalue: BIGINT,
            state: BIGINT,
            state_description: STRING,
            tp_Id: BIGINT,
            tsn: BIGINT,
            type_use: BIGINT,
            uid_target: BIGINT,
            validator_id: BIGINT
        >>
    >
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funciones auxiliares

# COMMAND ----------

def qualified_name(tabla: str) -> str:
    """Devuelve el nombre completo en Unity Catalog: catalogo.trx_mifare.tabla"""
    return f"`{CATALOG}`.{SCHEMA}.{tabla}"


def bytes_in(ruta: str) -> int:
    """Suma el tamaño en bytes de todos los archivos dentro de una carpeta."""
    total = 0
    for f in dbutils.fs.ls(ruta):
        total += bytes_in(f.path) if f.name.endswith("/") else f.size
    return total


def size_mb(ruta: str) -> float:
    """Calcula el tamaño de una carpeta en MB."""
    try:
        return round(bytes_in(ruta) / 1024 / 1024, 2)
    except Exception:
        return 0.0


def get_max_id(tabla: str, default_id: int = API_DEFAULT_TRX_ID) -> int:
    """Obtiene el máximo ID registrado en Silver/Bronze para paginación incremental en la API."""
    full_table = qualified_name(tabla)
    if spark.catalog.tableExists(full_table):
        res = spark.sql(f"SELECT MAX(id) FROM {full_table}").first()[0]
        if res is not None:
            return int(res)
    return default_id


def read_raw(ruta: str = RAW_GLOB):
    """Lee los JSON de RAW con esquema explícito y agrega columnas de auditoría."""
    return (spark.read.format("json")
            .option("multiLine", True)
            .schema(ESQUEMA_USES)
            .load(ruta)
            .select("*",
                    F.col("_metadata.file_path").alias("archivo_origen"),
                    F.current_timestamp().alias("ingestado_en")))

# COMMAND ----------

print("✔ Setup listo")
print(f"  Catálogo / Esquema : {CATALOG}.{SCHEMA}")
print(f"  Volumen RAW        : {BASE}")
print(f"  Ruta JSON Uses     : {RAW_PATH}")
print(f"  TransactionID base : {API_DEFAULT_TRX_ID}")