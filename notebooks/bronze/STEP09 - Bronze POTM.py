# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.dbutils import *

from datetime import datetime, date


## Connect to storage account 
client_id = "97a93714-50d0-4ddd-a222-08b41cdea7dd"
client_secret = dbutils.secrets.get(scope="olist-kv-test1",key="secret-test")
tenant_id = "9ae9656f-f92e-4a40-a1d3-ecdc2768584d"

storage_account = "ipldatastorageaccount"


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



container = 'bronze'
checkpoint_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/potm/checkpoints/'
schema_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/potm/schema/'

# COMMAND ----------

season = 2022

# COMMAND ----------

try:
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format","csv") \
        .option("cloudFiles.schemaLocation",schema_path) \
        .option("checkpointLocation",checkpoint_path) \
        .option("header","true") \
        .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/potm/landing")
    print("Read Sucess")
except Exception as e:
    print("Read Failed",e)

df = (
    df
    .withColumn(
        "file_name",
        input_file_name()
    )
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
    .withColumn(
        "season",
        lit(season)
    )
)

# COMMAND ----------

### Write stream

try:
    df.writeStream \
        .format("delta") \
        .option("checkpointLocation",checkpoint_path) \
        .outputMode("append") \
        .trigger(availableNow=True) \
        .option("mergeSchema","true") \
        .start(f"abfss://{container}@{storage_account}.dfs.core.windows.net/potm/delta")

    if not spark.catalog.tableExists("ipl_database.bronze.potm_raw"):
        spark.sql(
        f'''
        create table ipl_database.bronze.potm_raw
        using delta
        location "abfss://bronze@ipldatastorageaccount.dfs.core.windows.net/potm/delta"
        ''')
    else:
        spark.sql('REFRESH TABLE ipl_database.bronze.potm_raw')
    print("Write Complete!!!")
except Exception as e:
    print("Write Failed!!!",e)

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.read.format("delta").load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/potm/delta").display()

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from ipl_database.bronze.potm_raw