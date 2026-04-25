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
checkpoint_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/extra_runs/checkpoints/'
schema_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/extra_runs/schema/'

# COMMAND ----------

season = 2022

# COMMAND ----------

## Read stream

try:
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format","csv") \
        .option("header","true") \
        .option("cloudFiles.schemaLocation",schema_path) \
        .option("checkpointLocation",checkpoint_path) \
        .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/extra_runs/landing/")
    print("Read Complete!")
except Exception as e:
    print("Read Failed!!!",e)


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
        .start(f"abfss://{container}@{storage_account}.dfs.core.windows.net/extra_runs/delta")

    if not spark.catalog.tableExists("ipl_database.bronze.extra_runs_raw"):
        spark.sql(
        f'''
        create table ipl_database.bronze.extra_runs_raw
        using delta
        location "abfss://bronze@ipldatastorageaccount.dfs.core.windows.net/extra_runs/delta"
        ''')
    else:
        spark.sql('REFRESH TABLE ipl_database.bronze.extra_runs_raw')
    print("Write Complete!!!")
except Exception as e:
    print("Write Failed!!!",e)

# COMMAND ----------

# MAGIC %skip
# MAGIC try:
# MAGIC     if spark.catalog.tableExists("ipl_database.bronze.extra_runs_raw"):
# MAGIC         df.writeStream.mode("append").saveAsTable("ipl_database.bronze.extra_runs_raw")
# MAGIC         print("New data appended")
# MAGIC     else:
# MAGIC         df.writeStream \
# MAGIC         .format("delta") \
# MAGIC         .option("checkpointLocation",checkpoint_path) \
# MAGIC         .trigger(availableNow=True) \
# MAGIC         .option("mergeSchema","true") \
# MAGIC         .start(f"abfss://{container}@{storage_account}.dfs.core.windows.net/extra_runs/delta")
# MAGIC     spark.sql(
# MAGIC         f'''
# MAGIC         create table ipl_database.bronze.extra_runs_raw
# MAGIC         using delta
# MAGIC         location "abfss://bronze@ipldatastorageaccount.dfs.core.windows.net/extra_runs/delta"
# MAGIC         '''
# MAGIC     )
# MAGIC     print("Write Complete!!!")
# MAGIC
# MAGIC except Exception as e:
# MAGIC     print("Failed!!",e)