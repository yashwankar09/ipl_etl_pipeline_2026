# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.dbutils import *

# COMMAND ----------

## Connect to storage account 
client_id = "97a93714-50d0-4ddd-a222-08b41cdea7dd"
client_secret = dbutils.secrets.get(scope="olist-kv-test1",key="secret-test")
tenant_id = "9ae9656f-f92e-4a40-a1d3-ecdc2768584d"

storage_account = "ipldatastorageaccount"

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
  "OAuth"
)

spark.conf.set(
  f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
  f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
  client_id
)

spark.conf.set(
  f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
  client_secret
)

spark.conf.set(
  f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
  f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------

container = 'bronze'
checkpoint_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/batting/checkpoints/'
schema_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/batting/schema/'

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC try:
# MAGIC     batting_rs_df = spark.readStream.format("cloudFiles") \
# MAGIC         .option("cloudFiles.format","csv") \
# MAGIC         .option("header","true") \
# MAGIC         .option("cloudFiles.schemaLocation",schema_path) \
# MAGIC         .option("pathGlobFilter", "batsman_data_1st_Match_N_.csv") \
# MAGIC         .option("checkpointLocation", checkpoint_path) \
# MAGIC         .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/batting/landing/")
# MAGIC     print("File loaded sucessfully!")
# MAGIC except Exception as e:
# MAGIC     print(f"File not loaded.{e}")
# MAGIC
# MAGIC batting_rs_df = (
# MAGIC     batting_rs_df
# MAGIC     .withColumn(
# MAGIC         "file_name",
# MAGIC         input_file_name()
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "ingestion_time",
# MAGIC         current_timestamp()
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # batting_rs_df.display()

# COMMAND ----------

## Reading data
try:
    batting_rs_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format","csv") \
        .option("header","true") \
        .option("cloudFiles.schemaLocation",schema_path) \
        .option("checkpointLocation", checkpoint_path) \
        .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/batting/landing/")
    print("File loaded sucessfully!")
except Exception as e:
    print(f"File not loaded.{e}")

batting_rs_df = (
    batting_rs_df
    .withColumn(
        "file_name",
        input_file_name()
    )
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
)

# COMMAND ----------

try:
    batting_rs_df.writeStream \
        .format("delta") \
        .option("checkpointLocation",checkpoint_path) \
        .option("mergeSchema","true") \
        .trigger(availableNow=True) \
        .start(f"abfss://{container}@{storage_account}.dfs.core.windows.net/batting/delta/")
    print("File written sucessfully!")
except Exception as e:
    print(f"File not written.{e}")

# COMMAND ----------

# MAGIC %skip
# MAGIC df = spark.read.format("delta") \
# MAGIC     .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/batting/delta/")
# MAGIC df.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df.select("ingestion_time").distinct().show()

# COMMAND ----------

if not spark.catalog.tableExists("ipl_database.bronze.batsman_data_raw"):
    spark.sql('''
            create table ipl_database.bronze.batsman_data_raw
            using delta
            location "abfss://bronze@ipldatastorageaccount.dfs.core.windows.net/batting/delta/"
              ''')
    print("Table 'ipl_database.bronze.batsman_data_raw' is created.")
else:
    spark.sql('''
            refresh table ipl_database.bronze.batsman_data_raw
            ''')
    print("Table 'ipl_database.bronze.batsman_data_raw' is refreshed.")

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC -- Register as a table
# MAGIC
# MAGIC create table ipl_database.bronze.batsman_data_raw
# MAGIC using delta
# MAGIC location "abfss://bronze@ipldatastorageaccount.dfs.core.windows.net/batting/delta/"