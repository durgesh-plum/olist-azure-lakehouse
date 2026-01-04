# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Dim Date
# MAGIC
# MAGIC ## Grain
# MAGIC - One row per calendar date
# MAGIC
# MAGIC ## Description
# MAGIC - Date dimension for time-based analysis across fact tables
# MAGIC
# MAGIC ## Attributes
# MAGIC - Year, month, day
# MAGIC - Day of week
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

from pyspark.sql import functions as F

configure_adls_auth()


# COMMAND ----------

date_bounds = (
    spark.table("silver.orders")
    .select(
        F.min("order_purchase_timestamp").alias("min_date"),
        F.max("order_purchase_timestamp").alias("max_date")
    )
    .collect()[0]
)

start_date = date_bounds["min_date"].date()
end_date = date_bounds["max_date"].date()

# COMMAND ----------

date_df = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 day
        )
    ) AS date
""")


# COMMAND ----------

dim_date = (
    date_df
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.date_format("date", "EEEE"))
)


# COMMAND ----------

(
    dim_date
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.dim_date")

)

print("Gold dim_date written successfully.")
