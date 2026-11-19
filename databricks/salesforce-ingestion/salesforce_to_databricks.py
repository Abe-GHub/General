# Databricks notebook source
# MAGIC %md
# MAGIC # Salesforce API -> Databricks (Delta / Unity Catalog)
# MAGIC
# MAGIC Uses `simple-salesforce` for auth, REST (SOQL) queries, and Bulk API 2.0 extracts.
# MAGIC Lands data as Delta tables in Unity Catalog with an incremental MERGE on `SystemModstamp`.
# MAGIC
# MAGIC **Prereqs**
# MAGIC 1. A Salesforce Connected App (or External Client App) with OAuth enabled.
# MAGIC    For server-to-server, enable the *Client Credentials Flow* and assign a "Run As" integration user.
# MAGIC 2. A Databricks secret scope holding the credentials, for example:
# MAGIC    ```
# MAGIC    databricks secrets create-scope salesforce
# MAGIC    databricks secrets put-secret salesforce sf_consumer_key
# MAGIC    databricks secrets put-secret salesforce sf_consumer_secret
# MAGIC    databricks secrets put-secret salesforce sf_domain        # e.g. mycompany.my  (for mycompany.my.salesforce.com)
# MAGIC    # Only if using username/password instead:
# MAGIC    databricks secrets put-secret salesforce sf_username
# MAGIC    databricks secrets put-secret salesforce sf_password
# MAGIC    databricks secrets put-secret salesforce sf_security_token
# MAGIC    ```
# MAGIC 3. A UC catalog/schema and a Volume for Bulk API staging files.

# COMMAND ----------

# MAGIC %pip install simple-salesforce==1.12.6
# MAGIC %restart_python

# COMMAND ----------

# ---------- Config ----------
SECRET_SCOPE   = "salesforce"
CATALOG        = "main"
SCHEMA         = "salesforce_raw"
STAGING_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/staging"   # create with: CREATE VOLUME main.salesforce_raw.staging
AUTH_MODE      = "client_credentials"                     # or "password"
API_VERSION    = "61.0"

OBJECTS = ["Account", "Contact", "Opportunity", "Case"]   # add custom objects like "Member__c"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.staging")

# COMMAND ----------

# ---------- Authenticate ----------
from simple_salesforce import Salesforce

def get_secret(key):
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)

if AUTH_MODE == "client_credentials":
    # OAuth 2.0 Client Credentials flow (recommended for jobs; no user password stored)
    sf = Salesforce(
        consumer_key=get_secret("sf_consumer_key"),
        consumer_secret=get_secret("sf_consumer_secret"),
        domain=get_secret("sf_domain"),          # "mycompany.my" -> https://mycompany.my.salesforce.com
        version=API_VERSION,
    )
else:
    # Username + password + security token (simplest, but avoid for production)
    sf = Salesforce(
        username=get_secret("sf_username"),
        password=get_secret("sf_password"),
        security_token=get_secret("sf_security_token"),
        domain="login",                          # use "test" for a sandbox
        version=API_VERSION,
    )

print("Connected to:", sf.sf_instance)

# COMMAND ----------

# ---------- Quick test: small SOQL query via REST API ----------
import pandas as pd

result = sf.query_all("SELECT Id, Name, Industry, SystemModstamp FROM Account LIMIT 10")
pdf = pd.DataFrame(result["records"]).drop(columns="attributes", errors="ignore")
display(spark.createDataFrame(pdf))

# COMMAND ----------

# ---------- Helpers ----------
from pyspark.sql import functions as F
import os, shutil, uuid

# Bulk API 2.0 cannot return compound fields (address, location) or base64 blobs
UNSUPPORTED_BULK_TYPES = {"address", "location", "base64"}

def get_queryable_fields(object_name):
    """Use describe() so every standard and custom field is pulled without hardcoding."""
    desc = getattr(sf, object_name).describe()
    return [
        f["name"] for f in desc["fields"]
        if f["type"] not in UNSUPPORTED_BULK_TYPES
    ]

def get_watermark(table_fqn):
    """Last SystemModstamp loaded, or None for a first full load."""
    if not spark.catalog.tableExists(table_fqn):
        return None
    row = spark.sql(f"SELECT MAX(SystemModstamp) AS wm FROM {table_fqn}").first()
    return row["wm"]

def bulk_extract(object_name, soql):
    """
    Bulk API 2.0 query -> CSV files in a UC Volume -> Spark DataFrame.
    Handles millions of rows; Salesforce paginates results into multiple files.
    """
    run_dir = f"{STAGING_VOLUME}/{object_name}/{uuid.uuid4()}"
    os.makedirs(run_dir, exist_ok=True)

    results = getattr(sf.bulk2, object_name).download(
        soql,
        path=run_dir,
        max_records=200000,       # rows per result file
    )
    total = sum(r["number_of_records"] for r in results)
    print(f"{object_name}: {total} records extracted to {run_dir}")

    if total == 0:
        return None, run_dir

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)     # long text fields can contain newlines
        .option("escape", '"')
        .csv(run_dir)
    )
    return df, run_dir

# COMMAND ----------

# ---------- Incremental load: Bulk API 2.0 -> Delta MERGE ----------
from delta.tables import DeltaTable

def load_object(object_name):
    table_fqn = f"{CATALOG}.{SCHEMA}.{object_name.lower()}"
    fields = get_queryable_fields(object_name)
    soql = f"SELECT {', '.join(fields)} FROM {object_name}"

    wm = get_watermark(table_fqn)
    if wm is not None:
        # SOQL datetime literals are unquoted ISO-8601 in UTC
        soql += f" WHERE SystemModstamp > {wm.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"

    df, run_dir = bulk_extract(object_name, soql)
    if df is None:
        print(f"{object_name}: no new or changed rows")
        return

    df = (
        df.withColumn("SystemModstamp", F.to_timestamp("SystemModstamp"))
          .withColumn("_ingested_at", F.current_timestamp())
    )

    if not spark.catalog.tableExists(table_fqn):
        df.write.format("delta").saveAsTable(table_fqn)
        print(f"{object_name}: created {table_fqn}")
    else:
        (
            DeltaTable.forName(spark, table_fqn).alias("t")
            .merge(df.alias("s"), "t.Id = s.Id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"{object_name}: merged into {table_fqn}")

    shutil.rmtree(run_dir, ignore_errors=True)   # clean up staging files

# Note: all columns land as STRING from CSV; cast in your silver layer (or in dbt).
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # tolerate new SF fields

for obj in OBJECTS:
    load_object(obj)

# COMMAND ----------

# ---------- Optional: capture hard deletes ----------
# Bulk 2.0 "queryAll" includes soft-deleted rows (IsDeleted = true) still in the recycle bin.
def load_deletes(object_name, since_days=7):
    table_fqn = f"{CATALOG}.{SCHEMA}.{object_name.lower()}"
    soql = (f"SELECT Id FROM {object_name} WHERE IsDeleted = true "
            f"AND SystemModstamp = LAST_N_DAYS:{since_days}")
    res = getattr(sf.bulk2, object_name).download(
        soql, path=f"{STAGING_VOLUME}/{object_name}_deletes", include_deleted=True
    )
    if sum(r["number_of_records"] for r in res) == 0:
        return
    deleted = spark.read.option("header", True).csv(f"{STAGING_VOLUME}/{object_name}_deletes")
    DeltaTable.forName(spark, table_fqn).alias("t").merge(
        deleted.alias("d"), "t.Id = d.Id"
    ).whenMatchedDelete().execute()
    shutil.rmtree(f"{STAGING_VOLUME}/{object_name}_deletes", ignore_errors=True)

# for obj in OBJECTS:
#     load_deletes(obj)

# COMMAND ----------

# ---------- Optional: raw REST call for anything simple-salesforce doesn't wrap ----------
# e.g. Reports API, Composite API, limits
limits = sf.restful("limits/")
print("Daily API requests remaining:", limits["DailyApiRequests"]["Remaining"])
print("Daily Bulk API 2.0 jobs remaining:", limits.get("DailyBulkV2QueryJobs", {}).get("Remaining"))
