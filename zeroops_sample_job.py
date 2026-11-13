# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ZeroOps Tutorial: Orders by Region (Sample Job)
# MAGIC
# MAGIC This notebook is the **job that Genie ZeroOps monitors** during the tutorial. It runs as the task of the job **zeroops_tutorial_YOUR_USER_NAME**, which the classroom setup created for you.
# MAGIC
# MAGIC It is a small, self-contained pipeline with no external files:
# MAGIC
# MAGIC 1. **Generate** 1,000 synthetic orders.
# MAGIC 2. **Clean** the orders and calculate the order amount.
# MAGIC 3. **Aggregate** revenue by region.
# MAGIC 4. **Show** the result as a DataFrame.
# MAGIC
# MAGIC Nothing is written to Unity Catalog, so the job needs no catalog, schema, or table.
# MAGIC
# MAGIC ## How to introduce a bug
# MAGIC
# MAGIC Three bugs are planted in this notebook as **commented-out lines**. Each one sits directly below the correct line and is marked with an **ERROR** banner. Uncommenting a bug line replaces the correct line, so the job fails.
# MAGIC
# MAGIC | Bug | Where | What happens |
# MAGIC |---|---|---|
# MAGIC | **ERROR 1** | Step 2 | A misspelled column name. Spark cannot resolve the column. |
# MAGIC | **ERROR 2** | Step 3 | A misspelled Python variable name. Python raises a `NameError`. |
# MAGIC | **ERROR 3** | Step 2 | A strict cast of a text column that contains `N/A`. Spark cannot convert the value. |
# MAGIC
# MAGIC To turn a bug **on**, delete the `#` at the start of its line. To turn it **off**, put the `#` back. You can also click in the line and press **Cmd + /** (Mac) or **Ctrl + /** (Windows) to toggle it. Only turn on **one bug at a time**.
# MAGIC
# MAGIC **NOTE:** Each bug line starts with `#` and no space, so deleting the `#` leaves the line correctly aligned. A leading space would cause an `IndentationError` instead of the intended bug.
# MAGIC
# MAGIC **NOTE:** The notebook saves automatically. The job always runs the latest saved version of this notebook, so you do not need to do anything else after editing.

# COMMAND ----------

# DBTITLE 1,Step 1: Generate Synthetic Orders
from pyspark.sql import functions as F

# 1,000 orders spread across 3 regions and the last 30 days.
# discount_pct arrives as text from the source system, and some rows contain 'N/A'.
orders_raw = (
    spark.range(1, 1001)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (F.col("order_id") % 200) + 1)
    .withColumn("region", F.element_at(F.array(F.lit("AMER"), F.lit("EMEA"), F.lit("APJ")), (F.col("order_id") % 3 + 1).cast("int")))
    .withColumn("quantity", (F.col("order_id") % 5) + 1)
    .withColumn("unit_price", F.round(F.rand(seed=42) * 90 + 10, 2))
    .withColumn("discount_pct", F.when(F.col("order_id") % 25 == 0, F.lit("N/A")).otherwise(((F.col("order_id") % 4) * 5).cast("string")))
    .withColumn("order_date", F.date_sub(F.current_date(), (F.col("order_id") % 30).cast("int")))
)

print(f"Generated {orders_raw.count()} orders")

# COMMAND ----------

# DBTITLE 1,Step 2: Clean Orders and Calculate Amount
# Convert the text discount to a number. try_cast returns NULL for values like 'N/A', which become 0.
orders_clean = orders_raw.withColumn("discount_pct", F.coalesce(F.expr("try_cast(discount_pct AS INT)"), F.lit(0)))

# ============================================================================
# ERROR 3: Strict cast. Fails on the 'N/A' values in discount_pct.
#          Uncomment the next line to turn this bug on.
#orders_clean = orders_raw.withColumn("discount_pct", F.col("discount_pct").cast("int"))
# ============================================================================

# Order amount after discount.
orders_clean = orders_clean.withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2))

# ============================================================================
# ERROR 1: Column name typo. 'unit_prise' does not exist.
#          Uncomment the next line to turn this bug on.
#orders_clean = orders_clean.withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_prise") * (1 - F.col("discount_pct") / 100), 2))
# ============================================================================

# COMMAND ----------

# DBTITLE 1,Step 3: Aggregate Revenue by Region
orders_by_region = (
    orders_clean
    .groupBy("region")
    .agg(
        F.count("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("customer_count"),
        F.round(F.sum("order_amount"), 2).alias("total_revenue"),
        F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
    )
    .withColumn("refreshed_at", F.current_timestamp())
)

# ============================================================================
# ERROR 2: Variable name typo. 'order_clean' is never defined.
#          Uncomment the next line to turn this bug on.
#orders_by_region = order_clean.groupBy("region").agg(F.count("order_id").alias("order_count"))
# ============================================================================

# COMMAND ----------

# DBTITLE 1,Step 4: Show the Result
# collect() runs the whole pipeline, so any planted bug fails the job here at the latest.
results = orders_by_region.orderBy("region").collect()

print(f"Revenue by region ({len(results)} regions):")
for row in results:
    print(f"  {row.region}: {row.order_count} orders, {row.total_revenue} total revenue")

display(orders_by_region)
