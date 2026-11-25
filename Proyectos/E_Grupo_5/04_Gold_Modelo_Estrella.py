# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · GOLD: modelo estrella
# MAGIC
# MAGIC | Tabla | Tipo | Grano | Origen |
# MAGIC |---|---|---|---|
# MAGIC | `dim_ciudad` | Dimensión | una fila por ciudad | lista de referencia `CIUDADES` |
# MAGIC | `dim_fecha` | Dimensión | una fila por día del calendario | rango de fechas de silver |
# MAGIC | `fact_clima_diario` | Hechos | una fila por ciudad y día | **CDF de silver** + `MERGE` |
# MAGIC
# MAGIC Llaves: `ciudad_key` (entero de la dimensión) y `fecha_key` (AAAAMMDD).
# MAGIC Métricas: temperaturas, amplitud térmica, lluvia, horas de lluvia, viento y dos banderas
# MAGIC (`dia_lluvioso` = 1 mm o más, `dia_calor_extremo` = 35 °C o más).

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

build_gold()

# COMMAND ----------

display(spark.table(DIM_CIUDAD).orderBy("ciudad_key"))

# COMMAND ----------

display(spark.table(DIM_FECHA).orderBy("fecha_key").limit(10))

# COMMAND ----------

display(spark.table(FACT).orderBy(F.col("fecha_key").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preguntas de negocio (las mismas que responde el reporte de Power BI)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. ¿Cuál es la ciudad más lluviosa?
# MAGIC SELECT c.ciudad, round(sum(f.precipitacion_mm)) AS lluvia_total_mm, sum(f.dia_lluvioso) AS dias_lluviosos
# MAGIC FROM fact_clima_diario f JOIN dim_ciudad c USING (ciudad_key)
# MAGIC GROUP BY c.ciudad
# MAGIC ORDER BY lluvia_total_mm DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. ¿Cuál es el mejor mes para viajar a la costa Caribe? (menos días de lluvia)
# MAGIC SELECT d.mes, d.nombre_mes,
# MAGIC        round(avg(f.dia_lluvioso) * 100) AS pct_dias_lluviosos,
# MAGIC        round(avg(f.temp_max_c), 1) AS temp_max_promedio
# MAGIC FROM fact_clima_diario f
# MAGIC JOIN dim_ciudad c USING (ciudad_key)
# MAGIC JOIN dim_fecha d USING (fecha_key)
# MAGIC WHERE c.region = 'Caribe'
# MAGIC GROUP BY d.mes, d.nombre_mes
# MAGIC ORDER BY pct_dias_lluviosos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. ¿Qué ciudades tienen más días de calor extremo (35 °C o más)?
# MAGIC SELECT c.ciudad, sum(f.dia_calor_extremo) AS dias_calor_extremo, max(f.temp_max_c) AS record_c
# MAGIC FROM fact_clima_diario f JOIN dim_ciudad c USING (ciudad_key)
# MAGIC GROUP BY c.ciudad
# MAGIC ORDER BY dias_calor_extremo DESC

# COMMAND ----------

show_history(FACT)
