# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.dbutils import *

from datetime import datetime, date

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
checkpoint_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/bowling/checkpoints/'
schema_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/bowling/schema/'

# COMMAND ----------


try:
    bowling_rs_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format","csv") \
        .option("header","true") \
        .option("cloudFiles.schemaLocation",schema_path) \
        .option("checkpointLocation", checkpoint_path) \
        .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/bowling/landing/")
    print("File loaded sucessfully!")
except Exception as e:
    print(f"File not loaded.{e}")

bowling_rs_df = (
    bowling_rs_df
    .withColumn(
        "file_name",
        input_file_name()
    )
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
)

# batting_rs_df.display()

# COMMAND ----------

try:
    bowling_rs_df.writeStream \
        .format("delta") \
        .option("checkpointLocation",checkpoint_path) \
        .trigger(availableNow=True) \
        .option("mergeSchema","true") \
        .start(f"abfss://{container}@{storage_account}.dfs.core.windows.net/bowling/delta/")
    print("File written sucessfully!")
except Exception as e:
    print(f"File not written.{e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Register as delta table

# COMMAND ----------

if spark.catalog.tableExists("ipl_database.bronze.bowler_raw_data"):
    spark.sql(f'''
          refresh table ipl_database.bronze.bowler_raw_data
          ''')
else:
    spark.sql(f'''
          create table ipl_database.bronze.bowler_raw_data
          using delta
          location "abfss://bronze@ipldatastorageaccount.dfs.core.windows.net/bowling/delta/"
          ''')