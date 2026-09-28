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

# # 02_silver — ChowHound Order Data
# Depends only on `01_bronze` having run (reads `bronze.order_data_typed`). Second link of the bronze -> silver -> gold cascade.
# 
# **What silver guarantees to gold**
# 1. Formats standardized: business keys carry no formatting characters and no sign.
# 2. The two POS conventions are reconciled (see section 2) and every row is tagged with its `source_system`.
# 3. Data-quality gate: every row that reaches `silver.order_data_clean` has all business keys present and a unique primary key. Anything else goes to `silver.dq_quarantine` with the reason.
# 
# Because of (3), gold builds its dimensions from silver's distinct keys and derives every fact foreign key from the same keys: referential integrity holds by construction, so gold contains no orphan handling.

# PARAMETERS CELL ********************

# ---- PARAMETERS ----
BRONZE_TABLE     = "bronze.order_data_typed"
SILVER_TABLE     = "silver.order_data_clean"
QUARANTINE_TABLE = "silver.dq_quarantine"

# Load mode: "full" overwrites silver; "incremental" upserts on the primary key
LOAD_MODE = "full"   # "full" | "incremental"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# Source: typed bronze table
df_bronze = spark.read.table(BRONZE_TABLE)
print(f"Rows read from bronze: {df_bronze.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Source system tag (must happen BEFORE keys are cleaned)
# The legacy POS writes order numbers as dash-formatted text (`001-451541-0`); the new POS writes plain integers.
# `source_system` is lineage metadata used as a slicer.

# CELL ********************

# Legacy POS order numbers are dash-formatted, e.g. 001-451541-0
LEGACY_ORDER_PATTERN = r"^\s*\d{3}-\d+-\d+\s*$"

df = df_bronze.withColumn(
    "source_system",
    F.when(F.col("SERVICE_ORDER_NUMBER").cast("string").rlike(LEGACY_ORDER_PATTERN), "Legacy POS").otherwise("New POS"),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Key standardization and sign convention
# - Keys: parentheses, dashes and spaces are stripped and the value is upper-cased. This also strips the minus sign.
# - Sign convention: the new POS exports customer / restaurant / warehouse ids and the quantities as negative numbers; `abs()` is applied to ids and quantities only.
# - Amounts are not passed through `abs()`.
# 
# **SCALABILITY:** native Spark functions only (no Python UDFs), so the work stays in Catalyst.

# CELL ********************

# Strip parentheses, dashes and spaces (this also removes the sign); upper-case; empty -> NULL
def clean_key(colname):
    c = F.regexp_replace(F.col(colname).cast("string"), r"[()\-\s]", "")
    return F.upper(F.when(c == "", None).otherwise(c))


def abs_quantity(colname):
    # Accounting-style "(-3.00)" is parsed first, then the sign convention is normalized.
    txt = F.regexp_replace(F.col(colname).cast("string"), r"[()]", "")
    return F.abs(txt.cast("double"))


df = (
    df
    .withColumn("service_order_number", clean_key("SERVICE_ORDER_NUMBER"))
    .withColumn("restaurant_location_id", clean_key("RESTAURANT_LOCATION_ID"))
    .withColumn("product_number", clean_key("PRODUCT_NUMBER"))
    .withColumn("customer_number", clean_key("CUSTOMER_NUMBER"))
    .withColumn("delivery_warehouse", clean_key("DELIVERY_WAREHOUSE"))
    .withColumn("original_quantity", abs_quantity("ORIGINAL_QUANTITY"))
    .withColumn("order_quantity", abs_quantity("ORDER_QUANTITY"))
    .withColumn("delivery_quantity", abs_quantity("DELIVERY_QUANTITY"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Typing and trimming of the remaining columns

# CELL ********************

# Types and text standardization
df = (
    df
    .withColumn("line_sequence", F.col("LINE_SEQUENCE").cast("int"))
    .withColumn("customer_rank", F.trim(F.col("CUSTOMER_RANK")))
    .withColumn("customer_rank", F.when(F.col("customer_rank") == "", "No Data").otherwise(F.col("customer_rank")))   # Blank rank -> "No Data"
    .withColumn("entry_date", F.to_date("ENTRY_DATE"))
    .withColumn("delivery_date", F.to_date("DELIVERY_DATE"))
    .withColumn("order_line_amount", F.col("ORDER_LINE_ITEM_TOTAL").cast("decimal(18,2)"))
    .withColumn("delivery_line_amount", F.col("DELIVERY_LINE_ITEM_TOTAL").cast("decimal(18,2)"))
    .withColumn("amount_lost", F.col("AMOUNT_LOST").cast("decimal(18,2)"))
    .withColumn("service_level_status", F.col("SERVICE_LEVEL_STATUS").cast("int"))
    .withColumn("update_flag", F.trim(F.col("UPDATE_FLAG")))
    .withColumn("region", F.trim(F.col("REGION")))
    .withColumn("warehouse_area", F.trim(F.col("WAREHOUSE_AREA")))
    .withColumn("warehouse_district", F.trim(F.col("WAREHOUSE_DISTRICT")))
    .withColumn("restaurant_area", F.trim(F.col("RESTAURANT_AREA")))
    .withColumn("restaurant_district", F.trim(F.col("RESTAURANT_DISTRICT")))
    .withColumn("reason_code", F.trim(F.col("REASON_CODE")))
    .withColumn("reason_code", F.when(F.col("reason_code") == "", "No Reason Code").otherwise(F.col("reason_code")))   # Blank reason -> "No Reason Code"
    # Lineage columns
    .withColumn("_load_mode", F.lit(LOAD_MODE))
    .withColumn("_ingested_at", F.current_timestamp())
)

# Output column order
final_columns = [
    "service_order_number", "line_sequence", "source_system",
    "restaurant_location_id", "customer_number", "customer_rank",
    "product_number", "delivery_warehouse",
    "entry_date", "delivery_date",
    "original_quantity", "order_quantity", "delivery_quantity",
    "order_line_amount", "delivery_line_amount", "amount_lost",
    "service_level_status", "update_flag", "reason_code",
    "region", "warehouse_area", "warehouse_district", "restaurant_area", "restaurant_district",
    "_load_mode", "_ingested_at",
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Data-quality gate (owns everything referential integrity needs)
# 1. **Key completeness:** service order number, line sequence, restaurant, product, warehouse and entry date must be present.
# 2. **Primary key uniqueness:** `(service_order_number, line_sequence)`; if repeated, the row with the latest delivery date is kept and the rest are quarantined.
# 
# Rejected rows are never dropped silently: they land in `silver.dq_quarantine` with a `dq_reason`.
# **SCALABILITY:** if quarantine volume jumps between runs, this table is the natural target for a Fabric Data Activator alert.

# CELL ********************

# Key completeness checks
key_checks = [
    ("MISSING_SERVICE_ORDER_NUMBER", F.col("service_order_number").isNull()),
    ("MISSING_LINE_SEQUENCE",        F.col("line_sequence").isNull()),
    ("MISSING_RESTAURANT_ID",        F.col("restaurant_location_id").isNull()),
    ("MISSING_PRODUCT_NUMBER",       F.col("product_number").isNull()),
    ("MISSING_WAREHOUSE",            F.col("delivery_warehouse").isNull()),
    ("MISSING_ENTRY_DATE",           F.col("entry_date").isNull()),
]

# First failing check wins (built in reverse so the first check ends up outermost).
dq_reason = F.lit(None).cast("string")
for reason, condition in reversed(key_checks):
    dq_reason = F.when(condition, reason).otherwise(dq_reason)

df_checked = df.select(*final_columns).withColumn("dq_reason", dq_reason)

df_key_failures = df_checked.filter(F.col("dq_reason").isNotNull())
df_candidates   = df_checked.filter(F.col("dq_reason").isNull())

# Primary key = (service_order_number, line_sequence); keep the latest delivery date, quarantine the rest
pk_window = Window.partitionBy("service_order_number", "line_sequence").orderBy(F.col("delivery_date").desc_nulls_last())
df_ranked = df_candidates.withColumn("_pk_rank", F.row_number().over(pk_window))

df_duplicates = (df_ranked.filter(F.col("_pk_rank") > 1)
                 .withColumn("dq_reason", F.lit("DUPLICATE_PRIMARY_KEY")).drop("_pk_rank"))
df_valid = df_ranked.filter(F.col("_pk_rank") == 1).drop("_pk_rank", "dq_reason")

# Rejected rows = failed key checks + duplicate primary keys
df_quarantine = df_key_failures.unionByName(df_duplicates)
print(f"Valid rows: {df_valid.count()} | Quarantined rows: {df_quarantine.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quarantine: rebuilt on full loads, accumulated on incremental loads
quarantine_mode = "overwrite" if LOAD_MODE == "full" else "append"
(df_quarantine.write.format("delta").mode(quarantine_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE))
print(f"{QUARANTINE_TABLE} written ({quarantine_mode})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Write silver — full vs incremental
# - `full`: overwrite (first / historical load).
# - `incremental`: upsert on the primary key so a later correction of the same order line updates it instead of duplicating it.

# CELL ********************

if LOAD_MODE == "full" or not spark.catalog.tableExists(SILVER_TABLE):
    # Full load (or first run): overwrite
    (df_valid.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE))
else:
    # Incremental: upsert on the primary key
    (DeltaTable.forName(spark, SILVER_TABLE).alias("t")
        .merge(df_valid.alias("s"),
               "t.service_order_number = s.service_order_number AND t.line_sequence = s.line_sequence")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

print("Silver updated in mode:", LOAD_MODE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Change notes (02_silver)
# - Negative ids and quantities from the new POS are a sign convention, not reversals. Evidence: no overlap with legacy rows, amounts never negative, 191 of 56,208 rows mirror a legacy line, fulfillment 85.6% legacy vs 86.7% new, median unit price $105.23 vs $106.12.
# - `abs()` is applied to ids and quantities only. Amounts are left untouched because they are already non-negative and a future genuine credit must not be erased.
# - The format pattern that tags `source_system` separates 12,692 legacy rows from 56,208 new-POS rows with no overlap.
# - The data-quality gate lives here (key completeness and primary-key uniqueness), so gold contains no orphan handling.
# 
# ### Layer assumptions
# 1. `TRACKING_NUMBER` is dropped because it equals `SERVICE_ORDER_NUMBER`.
# 2. There is no product master; the standardized value is assumed to identify the same product in both systems.
# 3. `DELIVERY_DATE` falls in Jan-Mar 2023 for every row; it is kept but is not reliable for lead time.
# 4. Coverage is not continuous across systems (9 restaurants in 2021, 1,490 in 2022, 5,645 in Q1-2023, about 860 in both), which limits year-over-year and lost-client conclusions.  

