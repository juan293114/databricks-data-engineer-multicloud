# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup del proyecto: Clima de Colombia
# MAGIC
# MAGIC Este notebook **no se corre solo**: los demás lo llaman con `%run ./00_Setup`.
# MAGIC Define la configuración (catálogo, esquema, volumen, rutas, tablas), la lista de ciudades y las funciones
# MAGIC de cada capa del medallón:
# MAGIC
# MAGIC | Capa | Qué guarda | Cómo se carga |
# MAGIC |---|---|---|
# MAGIC | **Raw** | JSON tal cual lo devuelve la API Open-Meteo | `requests` → Volumen |
# MAGIC | **Bronze** | Los mismos JSON en una tabla Delta | **Auto Loader** + `MERGE` |
# MAGIC | **Silver** | Una fila por ciudad y día, limpia y con tipos | **Change Data Feed** de bronze + `MERGE` |
# MAGIC | **Gold** | Modelo estrella: `dim_ciudad`, `dim_fecha`, `fact_clima_diario` | **Change Data Feed** de silver + `MERGE` |

# COMMAND ----------

import time, re, requests
from datetime import date, timedelta
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = spark.sql("SELECT current_catalog()").first()[0]   # 'workspace' en la Free Edition
SCHEMA  = "proyecto_clima"
VOLUME  = "datos"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SCHEMA} COMMENT 'Proyecto final: clima diario de ciudades de Colombia (Open-Meteo)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.{SCHEMA}.{VOLUME} COMMENT 'Archivos RAW: JSON de la API Open-Meteo'")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")

USER = spark.sql("SELECT current_user()").first()[0]         # recibe las alertas del Job

# COMMAND ----------

# Archivos (Volumen)
BASE            = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_PATH        = f"{BASE}/raw/clima"              # JSON tal cual llega: clima_<ciudad>_<desde>_<hasta>.json
SCHEMA_PATH     = f"{BASE}/autoloader/schema"      # Auto Loader guarda aquí el esquema
CHECKPOINT_PATH = f"{BASE}/autoloader/checkpoint"  # Auto Loader recuerda aquí qué archivos ya leyó

# Tablas (Delta)
BRONZE     = "bronze_clima"          # crudo: una fila por archivo JSON (Auto Loader)
SILVER     = "silver_clima_diario"   # limpio: una fila por ciudad y día (CDF de bronze + MERGE)
DIM_CIUDAD = "dim_ciudad"            # gold: dimensión de ciudades
DIM_FECHA  = "dim_fecha"             # gold: dimensión de fechas (calendario)
FACT       = "fact_clima_diario"     # gold: tabla de hechos (CDF de silver + MERGE)
STATE      = "pipeline_state"        # hasta qué versión del origen procesó cada capa
LOG        = "pipeline_log"          # bitácora de cada paso del Job

JOB_NAME = "proyecto_clima_colombia"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Las ciudades
# MAGIC Es la información de referencia del proyecto: con ella se pide el clima a la API y se arma `dim_ciudad`.
# MAGIC `ciudad_key` es la llave sustituta (surrogate key) de la dimensión.

# COMMAND ----------

CIUDADES = [
    # ciudad_key, codigo,         ciudad,          departamento,         region,      latitud,  longitud, altitud_m
    (1,  "bogota",       "Bogotá",        "Cundinamarca",       "Andina",     4.7110,  -74.0721, 2640),
    (2,  "medellin",     "Medellín",      "Antioquia",          "Andina",     6.2442,  -75.5812, 1495),
    (3,  "cali",         "Cali",          "Valle del Cauca",    "Pacífica",   3.4516,  -76.5320, 1018),
    (4,  "barranquilla", "Barranquilla",  "Atlántico",          "Caribe",    10.9639,  -74.7964,   18),
    (5,  "cartagena",    "Cartagena",     "Bolívar",            "Caribe",    10.3910,  -75.4794,    2),
    (6,  "santa_marta",  "Santa Marta",   "Magdalena",          "Caribe",    11.2408,  -74.1990,    6),
    (7,  "bucaramanga",  "Bucaramanga",   "Santander",          "Andina",     7.1193,  -73.1227,  959),
    (8,  "pereira",      "Pereira",       "Risaralda",          "Andina",     4.8133,  -75.6961, 1411),
    (9,  "cucuta",       "Cúcuta",        "Norte de Santander", "Andina",     7.8939,  -72.5078,  320),
    (10, "quibdo",       "Quibdó",        "Chocó",              "Pacífica",   5.6947,  -76.6611,   43),
    (11, "leticia",      "Leticia",       "Amazonas",           "Amazonía",  -4.2153,  -69.9406,   96),
    (12, "san_andres",   "San Andrés",    "San Andrés",         "Insular",   12.5847,  -81.7006,    2),
]
COLUMNAS_CIUDAD = ["ciudad_key", "ciudad_codigo", "ciudad", "departamento", "region", "latitud", "longitud", "altitud_m"]

# Variables diarias que se piden a la API
VARIABLES = ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
             "precipitation_sum", "precipitation_hours", "wind_speed_10m_max"]

# Esquema explícito del JSON de Open-Meteo (una respuesta = un objeto por ciudad)
ESQUEMA_CLIMA = """
    latitude DOUBLE, longitude DOUBLE, elevation DOUBLE,
    timezone STRING, utc_offset_seconds INT,
    daily_units STRUCT<
        time: STRING, temperature_2m_max: STRING, temperature_2m_min: STRING, temperature_2m_mean: STRING,
        precipitation_sum: STRING, precipitation_hours: STRING, wind_speed_10m_max: STRING
    >,
    daily STRUCT<
        time: ARRAY<STRING>,
        temperature_2m_max: ARRAY<DOUBLE>, temperature_2m_min: ARRAY<DOUBLE>, temperature_2m_mean: ARRAY<DOUBLE>,
        precipitation_sum: ARRAY<DOUBLE>, precipitation_hours: ARRAY<DOUBLE>, wind_speed_10m_max: ARRAY<DOUBLE>
    >
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilidades

# COMMAND ----------

def qualified_name(tabla):
    """Nombre completo de una tabla: catalogo.esquema.tabla"""
    return f"{CATALOG}.{SCHEMA}.{tabla}"


def table_version(tabla):
    """Última versión de una tabla Delta."""
    return spark.sql(f"DESCRIBE HISTORY {tabla}").agg(F.max("version")).first()[0]


def show_history(tabla):
    """Historial resumido de una tabla Delta."""
    display(spark.sql(f"DESCRIBE HISTORY {tabla}")
            .select("version", "timestamp", "operation", "operationMetrics")
            .orderBy("version"))


def log_step(paso, estado, detalle=""):
    """Escribe una línea en la bitácora del pipeline (pipeline_log)."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {LOG} (paso STRING, estado STRING, detalle STRING, registrado_en TIMESTAMP)")
    (spark.createDataFrame([(paso, estado, detalle)], "paso STRING, estado STRING, detalle STRING")
        .withColumn("registrado_en", F.current_timestamp())
        .write.mode("append").saveAsTable(LOG))
    print(f"[{paso}] {estado} {detalle}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW · API Open-Meteo → Volumen

# COMMAND ----------

ESPERAS_429 = [20, 60]   # segundos de espera antes de cada reintento si la API responde 429


def download_city(ciudad, desde, hasta):
    """Descarga el clima diario de una ciudad entre dos fechas a RAW. Devuelve 'nuevo', 'existía', 'falló' o 'limitada' (429 persistente)."""
    _, codigo, _, _, _, lat, lon, _ = ciudad
    destino = Path(RAW_PATH) / f"clima_{codigo}_{desde.isoformat()}_{hasta.isoformat()}.json"
    if destino.exists():
        return "existía"
    for intento in range(len(ESPERAS_429) + 1):
        try:
            r = requests.get("https://archive-api.open-meteo.com/v1/archive", params={
                "latitude": lat, "longitude": lon,
                "start_date": desde.isoformat(), "end_date": hasta.isoformat(),
                "daily": ",".join(VARIABLES), "timezone": "America/Bogota",
            }, timeout=60)
        except requests.RequestException as e:
            print(f"  ✗ {codigo}: {e}")
            return "falló"
        if r.status_code == 429 and intento == len(ESPERAS_429):
            print(f"  ✗ {codigo}: la API sigue respondiendo 429 después de reintentar")
            return "limitada"
        if r.status_code == 429:
            # 429 = "demasiadas llamadas". Open-Meteo limita por IP, y Databricks serverless
            # sale a internet por IPs compartidas con muchos usuarios: se espera y se reintenta.
            print(f"  … {codigo}: la API respondió 429 (límite de uso). Reintento en {ESPERAS_429[intento]} s")
            time.sleep(ESPERAS_429[intento])
            continue
        if r.status_code != 200:
            print(f"  ✗ {codigo}: HTTP {r.status_code} {r.text[:150]}")
            return "falló"
        break
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_bytes(r.content)          # el JSON exactamente como lo devolvió la API
    time.sleep(0.5)                         # cortesía con la API
    return "nuevo"


def download_all(desde, hasta):
    """Descarga el rango de fechas para todas las ciudades y resume el resultado.

    Si la API rechaza algunas descargas pero RAW ya tiene datos, avisa y sigue: el pipeline
    procesa lo que hay y los días que faltan se piden en la próxima corrida (el Job pide los
    últimos días cada vez). Solo falla si RAW quedó vacío, porque ahí no hay nada que procesar.
    """
    res = {"nuevo": 0, "existía": 0, "falló": 0}
    for i, ciudad in enumerate(CIUDADES):
        estado = download_city(ciudad, desde, hasta)
        if estado == "limitada":
            # El límite es de toda la IP: si una ciudad quedó bloqueada, las demás también. No se insiste.
            res["falló"] += len(CIUDADES) - i
            break
        res[estado] += 1
    print(f"Rango {desde} a {hasta} | Nuevos: {res['nuevo']} | Ya existían: {res['existía']} | Fallidos: {res['falló']}")
    if res["falló"]:
        if not raw_files():
            raise Exception(f"Fallaron {res['falló']} descargas de la API y RAW está vacío")   # el Job lo reintenta
        print(f"⚠ {res['falló']} descargas fallaron, pero RAW ya tiene {len(raw_files())} archivos: se sigue con lo que hay.")
    return res


def raw_files():
    """Nombres de los archivos JSON que hay en RAW, ordenados."""
    if not Path(RAW_PATH).exists():
        return []
    return sorted(f.name for f in dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE · Auto Loader (RAW → bronze_clima)

# COMMAND ----------

def create_bronze_table():
    """Crea bronze vacía con Change Data Feed activado (así los cambios se registran desde la versión 0)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            {ESQUEMA_CLIMA},
            _rescued_data STRING, source_file STRING, ingested_at TIMESTAMP
        ) TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)


def autoloader_read():
    """LECTURA · Auto Loader detecta en RAW los archivos nuevos y los lee con el esquema explícito."""
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")                              # cada archivo es un objeto JSON con saltos de línea
            .option("cloudFiles.schemaLocation", SCHEMA_PATH)
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # lo que no encaje en el esquema va aquí
            .schema(ESQUEMA_CLIMA)
            .load(RAW_PATH)
            .select("*",
                    F.col("_metadata.file_name").alias("source_file"),
                    F.current_timestamp().alias("ingested_at")))


def upsert_to_bronze(lote, lote_id):
    """Aplica un lote del stream en bronze con MERGE: un archivo = una fila (llave: source_file)."""
    from delta.tables import DeltaTable
    lote = lote.dropDuplicates(["source_file"])                       # MERGE exige una sola fila por clave
    (DeltaTable.forName(lote.sparkSession, qualified_name(BRONZE)).alias("t")
        .merge(lote.alias("s"), "t.source_file = s.source_file")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def run_bronze():
    """RAW → bronze: lectura + escritura (upsert) con Auto Loader."""
    create_bronze_table()
    antes = table_version(BRONZE)
    query = (autoloader_read().writeStream
             .foreachBatch(upsert_to_bronze)                          # lote a lote: MERGE en bronze
             .option("checkpointLocation", CHECKPOINT_PATH)
             .trigger(availableNow=True)                              # procesa lo pendiente y se detiene
             .start())
    query.awaitTermination()
    merges = (spark.sql(f"DESCRIBE HISTORY {BRONZE}")
              .where(f"version > {antes} AND operation = 'MERGE'")
              .select("operationMetrics").collect())
    nuevos = sum(int(m[0].get("numTargetRowsInserted", 0)) for m in merges)
    log_step("bronze", "OK", f"{nuevos} archivos nuevos cargados" if merges else "sin archivos nuevos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Data Feed · leer solo lo que cambió

# COMMAND ----------

def read_changes(tabla, desde, hasta):
    """Lee el Change Data Feed de una tabla entre dos versiones (ambas incluidas)."""
    return (spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", desde)
            .option("endingVersion", hasta)
            .table(tabla))


def last_processed(step):
    """Hasta qué versión del origen ya procesó este paso (-1 si nunca corrió)."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {STATE} (step STRING, last_version BIGINT, updated_at TIMESTAMP)")
    fila = spark.table(STATE).where(F.col("step") == step).first()
    return fila["last_version"] if fila else -1


def save_processed(step, version):
    """Guarda hasta qué versión procesó este paso."""
    spark.sql(f"""
        MERGE INTO {STATE} t
        USING (SELECT '{step}' AS step, {version} AS last_version, current_timestamp() AS updated_at) s
        ON t.step = s.step
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER · bronze → silver_clima_diario
# MAGIC Reglas de limpieza:
# MAGIC 1. **Aplanar**: la API entrega listas (`daily.time[]`, `daily.temperature_2m_max[]`…); se convierten en una fila por día.
# MAGIC 2. **Tipos**: la fecha pasa de texto a `DATE`; las medidas se redondean a 1 decimal.
# MAGIC 3. **Ciudad**: el código de la ciudad sale del nombre del archivo (`clima_<ciudad>_...json`).
# MAGIC 4. **Calidad**: se descartan días sin temperatura (la API devuelve `null` en días que aún no tiene) y
# MAGIC    días con datos imposibles (mínima mayor que máxima, lluvia negativa).
# MAGIC 5. **Sin duplicados**: si el mismo día de la misma ciudad llega en dos archivos, gana el más reciente.

# COMMAND ----------

SILVER_COLS = ["ciudad_codigo", "fecha", "temp_max_c", "temp_min_c", "temp_media_c",
               "precipitacion_mm", "horas_lluvia", "viento_max_kmh", "source_file", "ingested_at"]


def create_silver_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER} (
            ciudad_codigo STRING, fecha DATE,
            temp_max_c DOUBLE, temp_min_c DOUBLE, temp_media_c DOUBLE,
            precipitacion_mm DOUBLE, horas_lluvia DOUBLE, viento_max_kmh DOUBLE,
            source_file STRING, ingested_at TIMESTAMP
        ) TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)


def to_silver(df):
    """Aplana el JSON de bronze: una fila por ciudad y día, con tipos correctos y reglas de calidad."""
    extra = [c for c in ("_change_type", "_commit_version") if c in df.columns]
    dias = F.arrays_zip(
        F.col("daily.time").alias("fecha"),
        F.col("daily.temperature_2m_max").alias("tmax"),
        F.col("daily.temperature_2m_min").alias("tmin"),
        F.col("daily.temperature_2m_mean").alias("tmedia"),
        F.col("daily.precipitation_sum").alias("lluvia"),
        F.col("daily.precipitation_hours").alias("horas"),
        F.col("daily.wind_speed_10m_max").alias("viento"),
    )
    return (df
        .select(F.regexp_extract("source_file", r"^clima_(.+)_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.json$", 1).alias("ciudad_codigo"),
                F.explode(dias).alias("d"), "source_file", "ingested_at", *extra)
        .select("ciudad_codigo",
                F.expr("try_cast(d.fecha AS DATE)").alias("fecha"),
                F.round("d.tmax", 1).alias("temp_max_c"),
                F.round("d.tmin", 1).alias("temp_min_c"),
                F.round("d.tmedia", 1).alias("temp_media_c"),
                F.round("d.lluvia", 1).alias("precipitacion_mm"),
                F.round("d.horas", 1).alias("horas_lluvia"),
                F.round("d.viento", 1).alias("viento_max_kmh"),
                "source_file", "ingested_at", *extra)
        .where("ciudad_codigo <> '' AND fecha IS NOT NULL")
        .where("temp_max_c IS NOT NULL AND temp_min_c IS NOT NULL")       # días que la API aún no tiene
        .where("temp_min_c <= temp_max_c")                                # dato imposible
        .where("precipitacion_mm IS NULL OR precipitacion_mm >= 0"))      # dato imposible


def build_silver():
    """bronze → silver: aplica solo los cambios de bronze que aún no se procesaron."""
    create_silver_table()
    desde, hasta = last_processed("silver") + 1, table_version(BRONZE)
    if desde > hasta:
        log_step("silver", "OK", "al día: no hay cambios nuevos en bronze")
        return

    # 1. Cambios de bronze pendientes (solo inserciones y la imagen nueva de las actualizaciones)
    cambios = read_changes(BRONZE, desde, hasta).where("_change_type IN ('insert', 'update_postimage')")

    # 2. Aplanar y quedarse con el dato más reciente de cada ciudad y día
    ultimo = Window.partitionBy("ciudad_codigo", "fecha").orderBy(
        F.col("_commit_version").desc(), F.col("ingested_at").desc(), F.col("source_file").desc())
    filas = (to_silver(cambios)
             .withColumn("rn", F.row_number().over(ultimo)).where("rn = 1").drop("rn"))

    # 3. MERGE: si el día ya existe se actualiza, si no se inserta
    valores = {c: f"s.{c}" for c in SILVER_COLS}
    (DeltaTable.forName(spark, qualified_name(SILVER)).alias("t")
        .merge(filas.alias("s"), "t.ciudad_codigo = s.ciudad_codigo AND t.fecha = s.fecha")
        .whenMatchedUpdate(set=valores)
        .whenNotMatchedInsert(values=valores)
        .execute())

    # 4. Recordar hasta dónde llegamos
    save_processed("silver", hasta)
    log_step("silver", "OK", f"aplicados {filas.count()} días de bronze (versiones {desde} a {hasta})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD · modelo estrella
# MAGIC
# MAGIC ```
# MAGIC            dim_ciudad                     dim_fecha
# MAGIC        (ciudad_key, ciudad,          (fecha_key, fecha, anio, mes,
# MAGIC         departamento, region…)        nombre_mes, temporada…)
# MAGIC                  \                        /
# MAGIC                   \                      /
# MAGIC                    fact_clima_diario
# MAGIC        (ciudad_key, fecha_key, temp_max_c, temp_min_c,
# MAGIC         precipitacion_mm, dia_lluvioso, dia_calor_extremo…)
# MAGIC ```
# MAGIC Grano de la tabla de hechos: **una fila por ciudad y día**.

# COMMAND ----------

def build_dim_ciudad():
    """dim_ciudad: sale de la lista CIUDADES (datos de referencia). MERGE por ciudad_key."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DIM_CIUDAD} (
            ciudad_key INT, ciudad_codigo STRING, ciudad STRING, departamento STRING, region STRING,
            latitud DOUBLE, longitud DOUBLE, altitud_m INT, es_costa BOOLEAN
        )
    """)
    fuente = (spark.createDataFrame(CIUDADES, COLUMNAS_CIUDAD)
              .withColumn("ciudad_key", F.col("ciudad_key").cast("int"))
              .withColumn("altitud_m", F.col("altitud_m").cast("int"))
              .withColumn("es_costa", F.col("region").isin("Caribe", "Insular")))
    (DeltaTable.forName(spark, qualified_name(DIM_CIUDAD)).alias("t")
        .merge(fuente.alias("s"), "t.ciudad_key = s.ciudad_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    log_step("gold.dim_ciudad", "OK", f"{len(CIUDADES)} ciudades")


def build_dim_fecha():
    """dim_fecha: un calendario con todos los días entre la primera y la última fecha de silver."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DIM_FECHA} (
            fecha_key INT, fecha DATE, anio INT, trimestre INT, mes INT, nombre_mes STRING,
            dia INT, dia_semana INT, nombre_dia STRING, es_fin_de_semana BOOLEAN, temporada STRING
        )
    """)
    rango = spark.table(SILVER).agg(F.min("fecha").alias("desde"), F.max("fecha").alias("hasta")).first()
    meses = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
             "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    dias  = ["", "Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]   # dayofweek: 1 = domingo
    mapa_mes = F.create_map(*[x for i, m in enumerate(meses[1:], 1) for x in (F.lit(i), F.lit(m))])
    mapa_dia = F.create_map(*[x for i, d in enumerate(dias[1:], 1) for x in (F.lit(i), F.lit(d))])
    fuente = (spark.sql(f"SELECT explode(sequence(DATE'{rango['desde']}', DATE'{rango['hasta']}')) AS fecha")
        .select(F.date_format("fecha", "yyyyMMdd").cast("int").alias("fecha_key"),
                "fecha",
                F.year("fecha").alias("anio"),
                F.quarter("fecha").alias("trimestre"),
                F.month("fecha").alias("mes"),
                mapa_mes[F.month("fecha")].alias("nombre_mes"),
                F.dayofmonth("fecha").alias("dia"),
                F.dayofweek("fecha").alias("dia_semana"),
                mapa_dia[F.dayofweek("fecha")].alias("nombre_dia"),
                F.dayofweek("fecha").isin(1, 7).alias("es_fin_de_semana"),
                # Colombia: temporadas secas dic-mar y jun-ago; lluviosas abr-may y sep-nov
                F.when(F.month("fecha").isin(12, 1, 2, 3, 6, 7, 8), "Seca").otherwise("Lluviosa").alias("temporada")))
    (DeltaTable.forName(spark, qualified_name(DIM_FECHA)).alias("t")
        .merge(fuente.alias("s"), "t.fecha_key = s.fecha_key")
        .whenNotMatchedInsertAll()                                 # los días del calendario no cambian
        .execute())
    log_step("gold.dim_fecha", "OK", f"calendario {rango['desde']} a {rango['hasta']}")


def build_fact():
    """silver → fact_clima_diario: aplica solo los días de silver que cambiaron (CDF)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FACT} (
            fecha_key INT, ciudad_key INT,
            temp_max_c DOUBLE, temp_min_c DOUBLE, temp_media_c DOUBLE, amplitud_termica_c DOUBLE,
            precipitacion_mm DOUBLE, horas_lluvia DOUBLE, viento_max_kmh DOUBLE,
            dia_lluvioso INT, dia_calor_extremo INT
        )
    """)
    desde, hasta = last_processed("gold") + 1, table_version(SILVER)
    if desde > hasta:
        log_step("gold.fact", "OK", "al día: no hay cambios nuevos en silver")
        return

    # 1. Cambios de silver pendientes: la versión más reciente de cada ciudad y día
    ultimo = Window.partitionBy("ciudad_codigo", "fecha").orderBy(F.col("_commit_version").desc())
    cambios = (read_changes(SILVER, desde, hasta)
               .where("_change_type IN ('insert', 'update_postimage')")
               .withColumn("rn", F.row_number().over(ultimo)).where("rn = 1"))

    # 2. Cambiar las llaves naturales por las llaves de las dimensiones y calcular las métricas
    fuente = (cambios.join(spark.table(DIM_CIUDAD).select("ciudad_key", "ciudad_codigo"), "ciudad_codigo")
        .select(F.date_format("fecha", "yyyyMMdd").cast("int").alias("fecha_key"),
                "ciudad_key", "temp_max_c", "temp_min_c", "temp_media_c",
                F.round(F.col("temp_max_c") - F.col("temp_min_c"), 1).alias("amplitud_termica_c"),
                "precipitacion_mm", "horas_lluvia", "viento_max_kmh",
                (F.col("precipitacion_mm") >= 1).cast("int").alias("dia_lluvioso"),        # 1 mm o más
                (F.col("temp_max_c") >= 35).cast("int").alias("dia_calor_extremo")))      # 35 °C o más

    # 3. MERGE en la tabla de hechos
    (DeltaTable.forName(spark, qualified_name(FACT)).alias("t")
        .merge(fuente.alias("s"), "t.fecha_key = s.fecha_key AND t.ciudad_key = s.ciudad_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    # 4. Recordar hasta dónde llegamos
    save_processed("gold", hasta)
    log_step("gold.fact", "OK", f"aplicados {fuente.count()} días de silver (versiones {desde} a {hasta})")


def build_gold():
    """Las dos dimensiones primero (la tabla de hechos las necesita), luego los hechos."""
    build_dim_ciudad()
    build_dim_fecha()
    build_fact()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mantenimiento · OPTIMIZE + ZORDER BY + VACUUM

# COMMAND ----------

ZORDER = {
    SILVER: "ciudad_codigo, fecha",     # las consultas filtran por ciudad y por rango de fechas
    FACT:   "ciudad_key, fecha_key",
}


def run_maintenance():
    """Compacta archivos pequeños, ordena por las columnas más filtradas y borra archivos viejos."""
    for tabla, columnas in ZORDER.items():
        m = spark.sql(f"OPTIMIZE {tabla} ZORDER BY ({columnas})").select("metrics").first()[0]
        log_step(f"optimize.{tabla}", "OK", f"archivos quitados {m['numFilesRemoved']}, agregados {m['numFilesAdded']}")
    for tabla in (BRONZE, SILVER, DIM_CIUDAD, DIM_FECHA, FACT):
        spark.sql(f"VACUUM {tabla} RETAIN 168 HOURS")             # 7 días: se conserva el time travel de la semana
        log_step(f"vacuum.{tabla}", "OK", "retención 168 horas")

# COMMAND ----------

def reset_pipeline():
    """Deja el pipeline como nuevo: tablas y memoria de Auto Loader. RAW se conserva."""
    for t in (BRONZE, SILVER, DIM_CIUDAD, DIM_FECHA, FACT, STATE, LOG):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    for p in (SCHEMA_PATH, CHECKPOINT_PATH):
        dbutils.fs.rm(p, recurse=True)
    print("✔ Pipeline reiniciado (RAW intacto)")

# COMMAND ----------

print("✔ Setup listo")
print(f"  Esquema : {CATALOG}.{SCHEMA}")
print(f"  Archivos: {BASE}")
print(f"  Tablas  : {BRONZE} → {SILVER} → {DIM_CIUDAD} + {DIM_FECHA} + {FACT}")
