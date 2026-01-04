# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Fact Order Items
# MAGIC
# MAGIC ## Grain
# MAGIC - One row per order line (`order_id`, `order_item_id`)
# MAGIC
# MAGIC ## Measures
# MAGIC - price
# MAGIC - freight_value
# MAGIC - total_item_value
# MAGIC
# MAGIC ## Foreign Keys
# MAGIC - order_id
# MAGIC - product_id
# MAGIC - seller_id
# MAGIC - order_date_key
# MAGIC - shipping_date_key
# MAGIC
# MAGIC ## Guarantees
# MAGIC - No duplicate order lines
# MAGIC - All monetary values are non-negative
# MAGIC - Join-safe to all dimensions
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

from pyspark.sql import functions as F

configure_adls_auth()

# COMMAND ----------

items = spark.read.format("delta").load(silver_order_items_path)
orders = spark.read.format("delta").load(gold_fact_orders_path)
products = spark.read.format("delta").load(gold_dim_products_path)
sellers = spark.read.format("delta").load(gold_dim_sellers_path)
dates = spark.read.format("delta").load(gold_dim_date_path)


# COMMAND ----------

orders_lookup = orders.select(
    "order_id",
    "order_date_key",
    "geo_id"
).dropDuplicates(["order_id"])

date_lookup = (
    dates
    .select("date", "date_key")
    .dropDuplicates(["date"])
)


# COMMAND ----------

items_df = items.alias("i")
orders_df = orders_lookup.alias("o")
products_df = products.alias("p")
sellers_df = sellers.alias("s")
dates_df = date_lookup.alias("d")


# COMMAND ----------

seller_lookup = (
    spark.read.format("delta")
    .load(gold_dim_sellers_path)
    .dropDuplicates(["seller_id"])
)


# COMMAND ----------

   sellers_df = seller_lookup.alias("s")


# COMMAND ----------

fact_order_items = (
    items_df
    .join(
        orders_df,
        F.col("i.order_id") == F.col("o.order_id"),
        "left"
    )
    .join(
        products_df,
        F.col("i.product_id") == F.col("p.product_id"),
        "left"
    )
    .join(
        sellers_df,
        F.col("i.seller_id") == F.col("s.seller_id"),
        "left"
    )
    .join(
        dates_df,
        F.col("i.shipping_limit_date").cast("date") == F.col("d.date"),
        "left"
    )
    .select(
        F.col("i.order_id").alias("order_id"),
        F.col("i.order_item_id").alias("order_item_id"),
        F.col("p.product_id").alias("product_id"),
        F.col("s.seller_id").alias("seller_id"),
        F.col("o.order_date_key").alias("order_date_key"),
        F.col("d.date_key").alias("shipping_date_key"),
        F.col("o.geo_id").alias("geo_id"),
        F.col("i.price").alias("price"),
        F.col("i.freight_value").alias("freight_value"),
        (F.col("i.price") + F.col("i.freight_value")).alias("total_item_value"),
        F.col("i.silver_load_timestamp").alias("silver_load_timestamp")
    )
)


# COMMAND ----------

assert (
    fact_order_items
    .select("order_id", "order_item_id")
    .distinct()
    .count()
    == fact_order_items.count()
), "DQ failed: Duplicate order lines detected"


# COMMAND ----------

assert fact_order_items.filter(
    F.col("order_id").isNull() |
    F.col("order_item_id").isNull() |
    F.col("product_id").isNull() |
    F.col("seller_id").isNull()
).count() == 0, "DQ failed: Null business keys"


# COMMAND ----------

assert fact_order_items.filter(
    (F.col("price") < 0) |
    (F.col("freight_value") < 0)
).count() == 0, "DQ failed: Negative monetary values"


# COMMAND ----------

fact_order_items.count()


# COMMAND ----------

fact_order_items.select(
    F.sum("price"),
    F.sum("freight_value"),
    F.sum("total_item_value")
).display()


# COMMAND ----------

fact_order_items.groupBy("seller_id").count().orderBy(F.desc("count")).display()


# COMMAND ----------

(
    fact_order_items
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.fact_order_items")
)

print("Gold fact_order_items written successfully.")

