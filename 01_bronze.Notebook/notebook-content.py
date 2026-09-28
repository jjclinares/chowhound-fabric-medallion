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

# # 01_bronze - ChowHound Order Data
# Self-contained ingestion notebook: reads `Files/landing`, builds `bronze.order_data_raw` (all text) and `bronze.order_data_typed` (safe casts), and creates the control tables it needs.
# Downstream: `02_silver` reads `bronze.order_data_typed`.

# PARAMETERS CELL ********************

#  PARAMETERS (tag this cell as "parameters" in Fabric) 
LANDING_PATH    = "Files/landing/"                         # Landing folder for incoming files
HISTORICAL_FILE = "Files/landing/restaurant_order_data.xlsx"  # Landing folder for incoming files
PROCESSED_PATH  = "Files/landing/processed/"                # Archive folder for loaded files (incremental mode)
SHEET_NAME      = "order_data"                              # Worksheet that holds the order data

RAW_TABLE   = "bronze.order_data_raw"                       # Output tables: raw (all text), typed (safe casts), audit (one row per run)
TYPED_TABLE = "bronze.order_data_typed"
AUDIT_TABLE = "bronze.ingestion_audit_log"

LOAD_MODE = "full"                                          # Load mode: "full" overwrites; "incremental" appends new files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 0. Prerequisites
# Schemas and control tables are created if missing (idempotent, safe to re-run).

# CELL ********************

# Medallion schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS control")

# Control table: files already loaded (drives incremental loads)
spark.sql("""
CREATE TABLE IF NOT EXISTS control.processed_files (
    file_name STRING,
    processed_at TIMESTAMP
) USING DELTA
""")

# Control table: date watermark for future database / API sources
spark.sql("""
CREATE TABLE IF NOT EXISTS control.wm_control (
    table_name STRING,
    last_watermark_value DATE,
    last_run_datetime TIMESTAMP
) USING DELTA
""")

print("Schemas and control tables verified / created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Read the source
# - **full**: reads the historical file.
# - **incremental**: lists `LANDING_PATH` and reads only files not yet registered in `control.processed_files`.

# CELL ********************

import pandas as pd

# Read one Excel file and keep every value as text (preserves mixed legacy / new formats)
def read_excel_as_text(relative_path):
    # Connection variable: OneLake (abfss) path of the workbook
    workbook_path = f"abfss://8ae2f35a-88b5-4348-897d-09d731cd5007@onelake.dfs.fabric.microsoft.com/0241cb38-6edc-4bdf-87f1-433be207885f/Files/Landing/restaurant_order_data.xlsx"
    pdf = pd.read_excel(workbook_path, sheet_name=SHEET_NAME)
    pdf = pdf.where(pd.notnull(pdf), None)                           # Empty cells become NULL
    pdf = pdf.applymap(lambda x: str(x) if x is not None else None)  # Every value as text: raw keeps the source untouched
    return pdf


files_loaded_this_run = []

if LOAD_MODE == "full":
    pdf_total = read_excel_as_text(HISTORICAL_FILE.replace("Files/", "Files/"))
    files_loaded_this_run = [HISTORICAL_FILE.split("/")[-1]]

else:
    # Incremental: read only files not yet registered in control.processed_files
    already_processed = {r.file_name for r in spark.read.table("control.processed_files").select("file_name").collect()}

    items = notebookutils.fs.ls(LANDING_PATH)
    new_files = [f for f in items if (not f.isDir) and f.name not in already_processed and f.name.lower().endswith((".xlsx", ".xls"))]

    if not new_files:
        print("No new files in the landing folder.")
        pdf_total = pd.DataFrame()
    else:
        parts = []
        for f in new_files:
            relative_file_path = f"{LANDING_PATH}{f.name}"
            parts.append(read_excel_as_text(relative_file_path))
            files_loaded_this_run.append(f.name)
        pdf_total = pd.concat(parts, ignore_index=True)

print(f"Files read in this run: {files_loaded_this_run}")
print(f"Rows read: {len(pdf_total)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Write `bronze.order_data_raw`
# All-text copy of the source. Overwrite on full loads, append on incremental loads.

# CELL ********************

if len(pdf_total) > 0:
    df_raw = spark.createDataFrame(pdf_total)

    # Raw table: no typing, no cleaning
    write_mode = "overwrite" if LOAD_MODE == "full" else "append"
    (df_raw.write.format("delta").mode(write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(RAW_TABLE))

    print(f"{RAW_TABLE} updated in {write_mode} mode. Rows: {df_raw.count()}")
else:
    # Nothing new to write: reuse the existing raw table
    df_raw = spark.read.table(RAW_TABLE) if spark.catalog.tableExists(RAW_TABLE) else None
    print("No new rows to write in this run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Schema validation
# Fails the run if a column of the expected source contract is missing.

# CELL ********************

from pyspark.sql import functions as F

# Expected source contract (24 columns)
expected_columns = {
    "ENTRY_DATE","DELIVERY_DATE","CUSTOMER_RANK","CUSTOMER_NUMBER","RESTAURANT_LOCATION_ID",
    "DELIVERY_WAREHOUSE","PRODUCT_NUMBER","ORIGINAL_QUANTITY","ORDER_QUANTITY","DELIVERY_QUANTITY",
    "LINE_SEQUENCE","TRACKING_NUMBER","SERVICE_ORDER_NUMBER","SERVICE_LEVEL_STATUS",
    "ORDER_LINE_ITEM_TOTAL","DELIVERY_LINE_ITEM_TOTAL","AMOUNT_LOST","UPDATE_FLAG","REGION",
    "WAREHOUSE_AREA","WAREHOUSE_DISTRICT","RESTAURANT_AREA","RESTAURANT_DISTRICT","REASON_CODE",
}

df_bronze_raw = spark.read.table(RAW_TABLE)
current_columns = set(df_bronze_raw.columns)
missing_columns = expected_columns - current_columns
# Stop the run if the source structure changed
assert not missing_columns, f"Missing expected columns in bronze: {missing_columns}"
print("Schema validated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Safe typing -> `bronze.order_data_typed`
# Dates and numbers only; business rules start in `02_silver`. `ORIGINAL_QUANTITY` stays text because it contains an accounting-style value.

# CELL ********************

# Safe casts only
df_typed = (
    df_bronze_raw
    .withColumn("ENTRY_DATE", F.to_date("ENTRY_DATE"))
    .withColumn("DELIVERY_DATE", F.to_date("DELIVERY_DATE"))
    .withColumn("LINE_SEQUENCE", F.col("LINE_SEQUENCE").cast("int"))
    .withColumn("ORDER_QUANTITY", F.col("ORDER_QUANTITY").cast("double"))
    .withColumn("DELIVERY_QUANTITY", F.col("DELIVERY_QUANTITY").cast("double"))
    .withColumn("ORDER_LINE_ITEM_TOTAL", F.col("ORDER_LINE_ITEM_TOTAL").cast("decimal(18,2)"))
    .withColumn("DELIVERY_LINE_ITEM_TOTAL", F.col("DELIVERY_LINE_ITEM_TOTAL").cast("decimal(18,2)"))
    .withColumn("AMOUNT_LOST", F.col("AMOUNT_LOST").cast("decimal(18,2)"))
    .withColumn("SERVICE_LEVEL_STATUS", F.col("SERVICE_LEVEL_STATUS").cast("int"))
    .withColumn("_ingested_at", F.current_timestamp())   # Lineage: load timestamp
)

write_mode = "overwrite" if LOAD_MODE == "full" else "append"
(df_typed.write.format("delta").mode(write_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(TYPED_TABLE))

print(f"{TYPED_TABLE} updated in {write_mode} mode. Rows: {df_typed.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Close the run
# Writes the audit row and, for incremental loads, archives the files and registers them.
# **SCALABILITY:** alert on `control.processed_files` growth with Fabric Data Activator.

# CELL ********************

# Audit trail: one row per run
audit_row = spark.createDataFrame(
    [(LOAD_MODE, len(pdf_total) if len(pdf_total) > 0 else 0)],
    ["load_mode", "rows_ingested"]
).withColumn("run_timestamp", F.current_timestamp())
audit_row.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)

if LOAD_MODE == "incremental" and files_loaded_this_run:
    # Archive loaded files and register them so the next run skips them
    for file_name in files_loaded_this_run:
        source_path = f"{LANDING_PATH}{file_name}"
        target_path = f"{PROCESSED_PATH}{file_name}"
        notebookutils.fs.mv(source_path, target_path, create_path=True)

    control_rows = [(file_name,) for file_name in files_loaded_this_run]
    spark.createDataFrame(control_rows, ["file_name"]) \
        .withColumn("processed_at", F.current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable("control.processed_files")

    print(f"Files moved to {PROCESSED_PATH} and registered in control.processed_files: {files_loaded_this_run}")

print("01_bronze completed in mode:", LOAD_MODE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verification: control tables after the run
display(spark.sql("SHOW TABLES IN control"))
display(spark.read.table("control.wm_control"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Change notes (01_bronze)
# - The former standalone landing loader is merged into this notebook, so the bronze layer runs with no earlier step.
# - Control tables (`control.processed_files`, `control.wm_control`) are created here, idempotently, instead of by a separate SQL script.
# - Incremental file handling (list, register, archive) lives in this notebook, not in a pipeline. The pipeline is not part of the submitted execution path.
# - Bronze raw keeps every value as text so the mixed legacy / new POS formats survive; typing is limited to dates and numbers.
# 
# ### Layer assumptions
# 1. `TRACKING_NUMBER` equals `SERVICE_ORDER_NUMBER` in every row reviewed; the duplicate is dropped in silver.
# 2. In incremental mode any `.xlsx` / `.xls` file in `Files/landing/` (outside `processed/`) is a new load.
# 3. The file name is the control key; a replaced file with the same name would not be detected (name + hash would fix it if that became a risk).
