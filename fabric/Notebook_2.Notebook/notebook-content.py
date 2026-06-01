# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date
import notebookutils

# ── Config via Variable Library ──────────────────────────────────────────────
lib = notebookutils.variableLibrary.getLibrary("config")

LAKEHOUSE_ID = lib["lh_source_id"]
WORKSPACE_ID = lib["workspace_id"]
ENVIRONMENT  = lib["env"]

print(f"Environment  : {ENVIRONMENT}")
print(f"Workspace ID : {WORKSPACE_ID}")
print(f"Lakehouse ID : {LAKEHOUSE_ID}")

# ── Schema & Table ────────────────────────────────────────────────────────────
SCHEMA     = "dbo"
TABLE_NAME = "dummy_lookup_test"

# ── Paths ─────────────────────────────────────────────────────────────────────
TABLES_BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables"
TABLE_PATH  = f"{TABLES_BASE}/{SCHEMA}/{TABLE_NAME}"

print(f"Table path   : {TABLE_PATH}")

# ── Dummy Data ────────────────────────────────────────────────────────────────
data = [
    Row(id=1, code="PROD-001", description="Widget Alpha",    category="Hardware", is_active=True,  created_date=date(2024, 1, 15)),
    Row(id=2, code="PROD-002", description="Widget Beta",     category="Hardware", is_active=True,  created_date=date(2024, 2, 20)),
    Row(id=3, code="SVC-001",  description="Support Basic",   category="Service",  is_active=True,  created_date=date(2024, 3, 10)),
    Row(id=4, code="SVC-002",  description="Support Premium", category="Service",  is_active=False, created_date=date(2024, 4, 5)),
    Row(id=5, code="PROD-003", description="Widget Gamma",    category="Software", is_active=True,  created_date=date(2024, 5, 1)),
]

schema = StructType([
    StructField("id",           IntegerType(), False),
    StructField("code",         StringType(),  False),
    StructField("description",  StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("is_active",    StringType(),  True),
    StructField("created_date", DateType(),    True),
])

# ── Write Delta files ─────────────────────────────────────────────────────────
df = spark.createDataFrame(data, schema=schema)

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(TABLE_PATH)

print(f"✅ [{ENVIRONMENT}] Written {df.count()} rows to {TABLE_PATH}")

# ── Read back via abfss ───────────────────────────────────────────────────────
df_read = spark.read.format("delta").load(TABLE_PATH)
df_read.createOrReplaceTempView("vw_dummy_lookup_test")

print(f"✅ [{ENVIRONMENT}] Read back {df_read.count()} rows")
spark.sql("SELECT * FROM vw_dummy_lookup_test").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
