# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0e23435-d0ae-48c2-81c4-eb5146cb1204",
# META       "default_lakehouse_name": "lh_source",
# META       "default_lakehouse_workspace_id": "284cbbbd-3f62-4535-befa-acbef052fdf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0e23435-d0ae-48c2-81c4-eb5146cb1204"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

# ── Config ──────────────────────────────────────────────────────────────────
SCHEMA     = "dbo"
TABLE_NAME = "dummy_lookup_test"

# ── Dummy Data ───────────────────────────────────────────────────────────────
data = [
    Row(id=1, code="PROD-001", description="Widget Alpha",   category="Hardware", is_active=True,  created_date=date(2024, 1, 15)),
    Row(id=2, code="PROD-002", description="Widget Beta",    category="Hardware", is_active=True,  created_date=date(2024, 2, 20)),
    Row(id=3, code="SVC-001",  description="Support Basic",  category="Service",  is_active=True,  created_date=date(2024, 3, 10)),
    Row(id=4, code="SVC-002",  description="Support Premium",category="Service",  is_active=False, created_date=date(2024, 4, 5)),
    Row(id=5, code="PROD-003", description="Widget Gamma",   category="Software", is_active=True,  created_date=date(2024, 5, 1)),
]

schema = StructType([
    StructField("id",           IntegerType(), False),
    StructField("code",         StringType(),  False),
    StructField("description",  StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("is_active",    StringType(),  True),
    StructField("created_date", DateType(),    True),
])

# ── Write ────────────────────────────────────────────────────────────────────
df = spark.createDataFrame(data, schema=schema)

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{SCHEMA}.{TABLE_NAME}")

print(f"✅ Table [{SCHEMA}.{TABLE_NAME}] created with {df.count()} records.")
spark.sql(f"SELECT * FROM {SCHEMA}.{TABLE_NAME}").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
