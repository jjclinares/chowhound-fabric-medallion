# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0241cb38-6edc-4bdf-87f1-433be207885f",
# META       "default_lakehouse_name": "lh_chowhoundltd_orders",
# META       "default_lakehouse_workspace_id": "8ae2f35a-88b5-4348-897d-09d731cd5007",
# META       "known_lakehouses": [
# META         {
# META           "id": "0241cb38-6edc-4bdf-87f1-433be207885f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 03_gold — Star schema for the Restaurant Profile semantic model
# Depends only on `02_silver` having run (reads `silver.order_data_clean`).
# 
# **Gold contract**
# - Gold supplies clean, conformed **data**: keys, descriptive attributes and the base numeric columns.
# - Anything that aggregates, compares periods, ranks, forecasts or projects is a **DAX measure**, not a column.
# - A derived column is allowed only when a visual needs it as a slicer / axis / legend (a measure cannot be one). Each of those is listed in the last cell.
# - No orphan handling here: silver guarantees complete, unique keys, and every foreign key below is the same hash of the same business key used by its dimension, so every fact row resolves by construction.


# PARAMETERS CELL ********************

# ---- PARAMETERS ----
SILVER_TABLE = "silver.order_data_clean"
GOLD_SCHEMA  = "gold"

# Load mode: "full" overwrites gold tables; "incremental" upserts on each table's key
LOAD_MODE = "full"   # "full" | "incremental"

LOST_CLIENT_DAYS       = 90     # Days without orders before a restaurant is labeled Lost Client
DATE_EXTENSION_MONTHS  = 12     # calendar extends this many months past the last date in the data
PARTITION_FACT_BY_YEAR = False  # SCALABILITY: switch on when the fact grows large

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# Source: clean silver table
df_silver = spark.read.table(SILVER_TABLE)


def sk(*cols):
    """Surrogate key: truncated SHA-256 of the standardized business key(s)."""
    return F.substring(F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols]), 256), 1, 16)


def strip_code_prefix(colname):
    """Source text comes as '<code>-<name>' (e.g. 'K10-United Kingdom'); keep the name."""
    return F.trim(F.regexp_replace(F.col(colname), r"^[A-Za-z0-9]+-", ""))


def most_frequent(df, key_col, value_col):
    """Most frequent value of value_col per key_col (ties: alphabetical). Keeps the column name."""
    w_count = Window.partitionBy(key_col, value_col)
    w_rank = Window.partitionBy(key_col).orderBy(F.desc("_cnt"), F.asc(value_col))
    return (df.select(key_col, value_col)
              .withColumn("_cnt", F.count("*").over(w_count))
              .withColumn("_rank", F.row_number().over(w_rank))
              .filter(F.col("_rank") == 1)
              .select(key_col, value_col))


# Write helper: overwrite on full loads; upsert on incremental loads when a merge key exists
def write_gold(df, table, merge_key=None, partition_by=None):
    target = f"{GOLD_SCHEMA}.{table}"
    if LOAD_MODE == "full" or merge_key is None or not spark.catalog.tableExists(target):
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.saveAsTable(target)
    else:
        (DeltaTable.forName(spark, target).alias("t")
            .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    print(f"{target}: {spark.read.table(target).count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 0. Housekeeping
# Drops gold tables that are not part of the semantic model.

# CELL ********************

# Gold tables outside the semantic model
for obsolete in ["dim_granularity", "fact_monthly_forecast"]:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{obsolete}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Dim_Date
# Full calendar years from the first year in the data to the year that contains *(last date + `DATE_EXTENSION_MONTHS`)*.
# The extension gives DAX contiguous future dates for same-period comparisons and for projecting months beyond the data.
# `week_start_date` (Monday) and `month_start_date` define weeks and months unambiguously for averages by period.
# It is a small reference table, overwritten on every run.

# CELL ********************

# Calendar bounds taken from the data
bounds = (df_silver.select(F.col("entry_date").alias("d"))
          .union(df_silver.select(F.col("delivery_date").alias("d")))
          .agg(F.min("d").alias("min_d"), F.max("d").alias("max_d")).first())

# Full calendar years, extended past the last date
dim_date = (
    spark.sql(f"""
        SELECT explode(sequence(
            make_date(year(DATE'{bounds.min_d}'), 1, 1),
            make_date(year(add_months(DATE'{bounds.max_d}', {DATE_EXTENSION_MONTHS})), 12, 31),
            interval 1 day)) AS date
    """)
    .withColumn("sk_date", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    .withColumn("month_start_date", F.trunc("date", "month"))
    .withColumn("week_start_date", F.date_trunc("week", "date").cast("date"))   # Grouping keys for weeks (Monday start) and months
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
)
write_gold(dim_date, "dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview
df = spark.sql("SELECT * FROM lh_chowhoundltd_orders.gold.dim_date LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Dim_Restaurant
# Grain: `restaurant_location_id` (one customer account owns many restaurants).
# Columns kept because a visual needs them as slicer / axis / legend (`client_status`, `restaurant_segment`, the geography names), plus the two base dates DAX needs (`first_order_date`, `last_order_date`).
# 
# Geography lives only here for restaurants (one clean version of each attribute). Warehouse geography is a different hierarchy and lives in `Dim_Warehouse`.

# CELL ********************

def mode_of(key_col, value_col):
    return most_frequent(df_silver, key_col, value_col)


# Reference date = last order date in the data
reference_date = df_silver.agg(F.max("entry_date")).first()[0]

# One row per restaurant: first / last order and lifetime amount
lifecycle = df_silver.groupBy("restaurant_location_id").agg(
    F.min("entry_date").alias("first_order_date"),
    F.max("entry_date").alias("last_order_date"),
    F.sum("order_line_amount").alias("_lifetime_amount"),   # Temporary: used only to build the segment
)

# Representative map point per restaurant district. The source has no coordinates, so this is small
# hand-authored reference data (one principal city per district). Three districts are business
# groupings, not places (080 Marine Management Finance, 908 House Accounts USA, 955 Government):
# they have no row here and stay NULL, so they do not plot on the map.
district_geo = spark.createDataFrame([
    ("210", 41.90, 12.50), ("218", 37.98, 23.73), ("219", 24.71, 46.68), ("220", 28.61, 77.21),
    ("410", 51.51, -0.13), ("420", 59.91, 10.75), ("430", 55.68, 12.57), ("470", 50.85, 4.35),
    ("489", 52.23, 21.01), ("490", 52.52, 13.40), ("495", 55.76, 37.62), ("810", 22.32, 114.17),
    ("820", 25.03, 121.57), ("830", 39.90, 116.41), ("850", 35.68, 139.69), ("860", 37.57, 126.98),
    ("880", 1.35, 103.82), ("902", 33.75, -84.39), ("903", 37.77, -122.42), ("904", 40.71, -74.01),
    ("905", 43.65, -79.38), ("906", 29.76, -95.37), ("925", -23.55, -46.63), ("945", -33.45, -70.67),
], "district_code string, restaurant_district_latitude double, restaurant_district_longitude double")

dim_restaurant = (
    lifecycle
    # Most frequent value per restaurant (ties: alphabetical)
    .join(mode_of("restaurant_location_id", "customer_number"), "restaurant_location_id", "left")
    .join(mode_of("restaurant_location_id", "customer_rank"), "restaurant_location_id", "left")
    .join(mode_of("restaurant_location_id", "restaurant_area"), "restaurant_location_id", "left")
    .join(mode_of("restaurant_location_id", "restaurant_district"), "restaurant_location_id", "left")
    .withColumn("restaurant_area_name", strip_code_prefix("restaurant_area"))
    .withColumn("restaurant_district_name", strip_code_prefix("restaurant_district"))
    .withColumn("_district_code", F.regexp_extract("restaurant_district", r"^([A-Za-z0-9]+)-", 1))
    .join(district_geo, F.col("_district_code") == F.col("district_code"), "left")
    .drop("restaurant_area", "restaurant_district", "_district_code", "district_code")
    .withColumn("_quartile", F.ntile(4).over(Window.orderBy("_lifetime_amount")))
    # Segment = quartile of lifetime order amount
    .withColumn(
        "restaurant_segment",
        F.when(F.col("_quartile") == 4, "Top Tier")
         .when(F.col("_quartile") == 3, "Core")
         .when(F.col("_quartile") == 2, "Emerging")
         .otherwise("Low Volume"))
    # Lost Client = no order in the last LOST_CLIENT_DAYS days before the reference date
    .withColumn(
        "client_status",
        F.when(F.datediff(F.lit(reference_date), F.col("last_order_date")) > LOST_CLIENT_DAYS, "Lost Client")
         .otherwise("Active Client"))
    .drop("_lifetime_amount", "_quartile")
    .withColumn("sk_restaurant", sk("restaurant_location_id"))
)
write_gold(dim_restaurant, "dim_restaurant", merge_key="restaurant_location_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview
df = spark.sql("SELECT * FROM lh_chowhoundltd_orders.gold.dim_restaurant LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Dim_Product

# CELL ********************

dim_product = (df_silver.select("product_number").distinct()
               .withColumn("sk_product", sk("product_number")))
write_gold(dim_product, "dim_product", merge_key="product_number")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview
df = spark.sql("SELECT * FROM lh_chowhoundltd_orders.gold.dim_product LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Dim_Warehouse
# The warehouse hierarchy is region -> area -> district. It is separate from the restaurant geography.

# CELL ********************

# One row per warehouse; most frequent hierarchy values
dim_warehouse = (
    df_silver.select("delivery_warehouse").distinct()
    .join(mode_of("delivery_warehouse", "warehouse_area"), "delivery_warehouse", "left")
    .join(mode_of("delivery_warehouse", "warehouse_district"), "delivery_warehouse", "left")
    .join(mode_of("delivery_warehouse", "region"), "delivery_warehouse", "left")
    .withColumn("warehouse_area_name", strip_code_prefix("warehouse_area"))
    .withColumn("warehouse_district_name", strip_code_prefix("warehouse_district"))
    .withColumn("region_name", strip_code_prefix("region"))
    .drop("warehouse_area", "warehouse_district", "region")
    .withColumn("sk_warehouse", sk("delivery_warehouse"))
)
write_gold(dim_warehouse, "dim_warehouse", merge_key="delivery_warehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview
df = spark.sql("SELECT * FROM lh_chowhoundltd_orders.gold.dim_warehouse LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Dim_Status / Dim_ReasonCode / Dim_MovementType
# Descriptions are inferred from data behaviour (no business dictionary available).

# CELL ********************

# Status 1 and -1 both mean delivered in full (the sign is the POS convention)
dim_status = spark.createDataFrame(
    [(1, "Delivered in full"), (-1, "Delivered in full"), (0, "Not delivered")],
    ["sk_status", "status_description"])
write_gold(dim_status, "dim_status")

# Movement descriptions are inferred; flag A is pending confirmation
dim_movement_type = spark.createDataFrame(
    [("C", "Standard line"), ("D", "Line not delivered"), ("A", "Flag A (definition pending)")],
    ["sk_movement_type", "movement_description"])
write_gold(dim_movement_type, "dim_movement_type")

# Reason description = source text without its numeric prefix
dim_reason_code = (
    df_silver.select(F.col("reason_code").alias("sk_reason_code")).distinct()
    .withColumn("reason_description",
                F.when(F.col("sk_reason_code") == "No Reason Code", F.col("sk_reason_code"))
                 .otherwise(strip_code_prefix("sk_reason_code")))
)
write_gold(dim_reason_code, "dim_reason_code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview
df = spark.sql("SELECT * FROM lh_chowhoundltd_orders.gold.dim_status LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview
df = spark.sql("SELECT * FROM lh_chowhoundltd_orders.gold.dim_reason_code LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview
df = spark.sql("SELECT * FROM lh_chowhoundltd_orders.gold.dim_movement_type LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Fact_Order_Lines
# Grain: one row per order line (`service_order_number` + `line_sequence`).
# Foreign keys are computed with the same `sk()` hash as the dimensions, so no lookup join is needed and no fact row can point to a missing dimension row.
# `entry_date` is kept as base data so DAX can do date arithmetic on the line directly (the 60-day look-ahead).
# 
# **SCALABILITY:** set `PARTITION_FACT_BY_YEAR = True` to partition the fact by `entry_year` once volume grows.

# CELL ********************

fact = (
    df_silver
    # Foreign keys use the same hash as the dimensions
    .withColumn("sk_order_line", sk("service_order_number", "line_sequence"))
    .withColumn("sk_restaurant", sk("restaurant_location_id"))
    .withColumn("sk_product", sk("product_number"))
    .withColumn("sk_warehouse", sk("delivery_warehouse"))
    .withColumn("sk_date_entry", F.date_format("entry_date", "yyyyMMdd").cast("int"))
    .withColumn("sk_date_delivery", F.date_format("delivery_date", "yyyyMMdd").cast("int"))
    # Fulfilled = delivered quantity equals ordered quantity
    .withColumn("fulfillment_status",
                F.when(F.col("order_quantity") == F.col("delivery_quantity"), "Fulfilled").otherwise("Not Fulfilled"))
)

fact_columns = [
    "sk_order_line", "service_order_number", "line_sequence", "source_system",
    "sk_restaurant", "sk_product", "sk_warehouse", "sk_date_entry", "sk_date_delivery",
    "service_level_status", "update_flag", "reason_code",
    "entry_date",
    "original_quantity", "order_quantity", "delivery_quantity",
    "order_line_amount", "delivery_line_amount", "amount_lost",
    "fulfillment_status",
]

# Optional partition column for large volumes
if PARTITION_FACT_BY_YEAR:
    fact = fact.withColumn("entry_year", F.year("entry_date"))
    fact_columns.append("entry_year")

write_gold(fact.select(*fact_columns), "fact_order_lines", merge_key="sk_order_line",
           partition_by="entry_year" if PARTITION_FACT_BY_YEAR else None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Change notes and design decisions (03_gold)
# - Removed from gold and moved to DAX: year-over-year volumes, restaurant age, lost-client flag, lifetime amount, the granularity table and the forecast table, and every derived flag on the fact except `fulfillment_status`.
# - Removed the customs-dependency placeholder: the source has no field for it.
# - Orphan handling was moved to silver.
# - Restaurant and warehouse geography stay in their own dimensions: the two code systems have no codes in common and `REGION` is determined by the warehouse area. A shared geography dimension would force a union of unrelated hierarchies.
# - Status descriptions come from data behaviour: `1` and `-1` both mean delivered in full (10,868 of 10,868 legacy and 48,725 of 48,726 new-POS rows delivered what was ordered); `0` always has delivery quantity 0; `D` always coincides with status `0`; flag `A` (23 rows) has no verifiable meaning.
# 
# ### Derived columns kept (each is needed as a slicer, axis or legend, which a measure cannot be)
# `fact_order_lines.fulfillment_status`, `fact_order_lines.source_system`, `dim_restaurant.client_status`, `dim_restaurant.restaurant_segment`, the geography names and district coordinates, and the two base dates `first_order_date` / `last_order_date`.
# 
# ### Assumptions
# - `LOST_CLIENT_DAYS = 90` is a working threshold.
# - District coordinates are hand-authored reference points (one principal city per district) because the source has none.

