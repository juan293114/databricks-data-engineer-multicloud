# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # C_Silver - CDF + limpieza + estandarización
# MAGIC
# MAGIC Silver consume exclusivamente las tablas Bronze mediante Change Data Feed (CDF).
# MAGIC Se conservan los cambios insert/update_postimage y se generan tablas Delta Silver.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")


def read_cdf(table_name):
    return (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)  
        .table(table_name)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


def write_table(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)  
    )


# Calendar
calendar = read_cdf(f"{BRONZE_SCHEMA}.calendar")
df_calendar_silver = (
    calendar
    .dropDuplicates()
    .dropna(subset=["date"])
    .withColumn("date", F.to_date("date"))
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("month", F.col("month").cast("int"))
    .withColumn("day", F.col("day").cast("int"))
    .withColumn("week", F.col("week").cast("int"))
    .withColumn("day_of_week", F.col("day_of_week").cast("int"))
)
write_table(df_calendar_silver, f"{SILVER_SCHEMA}.calendar")

# Products
products = read_cdf(f"{BRONZE_SCHEMA}.products")
df_products_silver = (
    products
    .dropDuplicates()
    .dropna(subset=["product_id"])
    .withColumn("cocoa_percent", F.col("cocoa_percent").cast("int"))
    .withColumn("weight_g", F.col("weight_g").cast("int"))
)
write_table(df_products_silver, f"{SILVER_SCHEMA}.products")

# Customers
customers = read_cdf(f"{BRONZE_SCHEMA}.customers")
df_customers_silver = (
    customers
    .dropDuplicates()
    .dropna(subset=["customer_id"])
    .withColumn("age", F.col("age").cast("int"))
    .withColumn("loyalty_member", F.col("loyalty_member").cast("int"))
    .withColumn("join_date", F.to_date("join_date"))
)
write_table(df_customers_silver, f"{SILVER_SCHEMA}.customers")

# Stores
stores = read_cdf(f"{BRONZE_SCHEMA}.stores")
df_stores_silver = (
    stores
    .dropDuplicates()
    .dropna(subset=["store_id"])
)
write_table(df_stores_silver, f"{SILVER_SCHEMA}.stores")

# Sales
sales = read_cdf(f"{BRONZE_SCHEMA}.sales")
df_sales_silver = (
    sales
    .dropDuplicates()
    .dropna(subset=["order_id", "order_date", "product_id", "store_id", "customer_id"])
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("discount", F.col("discount").cast("double"))
    .withColumn("revenue", F.col("revenue").cast("double"))
    .withColumn("cost", F.col("cost").cast("double"))
    .withColumn("profit", F.col("profit").cast("double"))
)
write_table(df_sales_silver, f"{SILVER_SCHEMA}.sales")

print("SILVER - CDF + limpieza + estandarización finalizados correctamente")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'calendar' AS tabla, COUNT(*) AS total_filas FROM workspace.silver.calendar
# MAGIC UNION ALL
# MAGIC SELECT 'products' AS tabla, COUNT(*) AS total_filas FROM workspace.silver.products
# MAGIC UNION ALL
# MAGIC SELECT 'customers' AS tabla, COUNT(*) AS total_filas FROM workspace.silver.customers
# MAGIC UNION ALL
# MAGIC SELECT 'stores' AS tabla, COUNT(*) AS total_filas FROM workspace.silver.stores
# MAGIC UNION ALL
# MAGIC SELECT 'sales' AS tabla, COUNT(*) AS total_filas FROM workspace.silver.sales;
# MAGIC
# MAGIC