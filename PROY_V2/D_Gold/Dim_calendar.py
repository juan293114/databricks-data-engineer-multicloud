# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # D_Gold - Modelo estrella
# MAGIC
# MAGIC Gold consume las tablas Silver registradas en Unity Catalog y construye
# MAGIC 4 dimensiones y 1 tabla de hechos, listas para Power BI.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

calendar = spark.table(f"{SILVER_SCHEMA}.calendar")



def write_gold(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


# Dimensión calendario
df_dim_calendar = (
    calendar
    .withColumn(
        "month_name",
        F.when(F.col("month") == 1, "Enero")
         .when(F.col("month") == 2, "Febrero")
         .when(F.col("month") == 3, "Marzo")
         .when(F.col("month") == 4, "Abril")
         .when(F.col("month") == 5, "Mayo")
         .when(F.col("month") == 6, "Junio")
         .when(F.col("month") == 7, "Julio")
         .when(F.col("month") == 8, "Agosto")
         .when(F.col("month") == 9, "Setiembre")
         .when(F.col("month") == 10, "Octubre")
         .when(F.col("month") == 11, "Noviembre")
         .when(F.col("month") == 12, "Diciembre")
    )
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
)
write_gold(df_dim_calendar, f"{GOLD_SCHEMA}.dim_calendar")


print("GOLD - tabla dimensional dim_calendar generado correctamente")
print("Dimensiones: dim_calendar")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_calendar' AS tabla, COUNT(*) AS total_filas FROM workspace.gold.dim_calendar
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dim_calendar limit 2