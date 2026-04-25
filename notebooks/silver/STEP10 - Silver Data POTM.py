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



container = 'silver'

# COMMAND ----------

## read raw data

df_raw = spark.table("ipl_database.bronze.potm_raw").filter(
    to_date(col("ingestion_time")) >= date.today()
)
df_raw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean raw data

# COMMAND ----------

df = (
  df_raw
  .dropDuplicates(['id'])
  .withColumn(
    "id",
    trim(col("id"))
  )
  .withColumn(
    "player",
    trim(col("player"))
  )
  .withColumn(
    "season",
    col("season").cast('int')
  )
  .select("id","player","season")
)


# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 Adding match UUID

# COMMAND ----------

match_uuid = spark.table("ipl_database.silver.match_details")

df_fin = (
  df.alias('a')
  .join(
    match_uuid.alias('b'),
    (col('a.id') == col('b.id')) & (col('a.season') == col('b.season')),
    'left'
  )
  .drop('id')
  .select('a.*','b.match_UUID')
)

# COMMAND ----------

# MAGIC %skip
# MAGIC df_fin.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC if not spark.catalog.tableExists("ipl_database.silver.potm"):
# MAGIC   df_fin.write.format("delta").mode("overwrite").save(f"abfss://{container}@{storage_account}.dfs.core.windows.net/potm")
# MAGIC   spark.sql('''
# MAGIC             create table ipl_database.silver.potm
# MAGIC             using delta
# MAGIC             location 'abfss://silver@ipldatastorageaccount.dfs.core.windows.net/potm'
# MAGIC             ''')
# MAGIC   print("write complete!!!")
# MAGIC else:
# MAGIC   df_fin.write.format("delta").mode("append").save(f"abfss://{container}@{storage_account}.dfs.core.windows.net/potm")
# MAGIC   spark.sql('refresh table ipl_database.silver.potm')
# MAGIC   print("Append Sucess!!!")

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC describe history ipl_database.silver.potm

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from ipl_database.silver.potm version as of 5

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC restore table ipl_database.silver.potm to version as of 5

# COMMAND ----------

# MAGIC %skip
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC delta_table = DeltaTable.forPath(spark, "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/potm")
# MAGIC
# MAGIC history_df = delta_table.history()
# MAGIC history_df.display()

# COMMAND ----------


# delta_table.restoreToVersion(5)

# COMMAND ----------

# spark.sql('refresh table ipl_database.silver.potm')

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from  ipl_database.silver.potm

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC drop table ipl_database.silver.potm

# COMMAND ----------

try:
    if spark.catalog.tableExists("ipl_database.silver.potm"):
        df_fin.createOrReplaceTempView("df_fin")
        spark.sql(f'''
                merge into ipl_database.silver.potm mc
                using df_fin nd
                on mc.player = nd.player and mc.season = nd.season
                when matched then update set *
                when not matched then insert *
                ''')
        # Upsert in not ideal here, switching to append process
        print("append sucessfull.")
    else:
        df_fin.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("abfss://silver@ipldatastorageaccount.dfs.core.windows.net/potm")
        spark.sql(f'''
                create table ipl_database.silver.potm
                using delta
                location "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/potm"
                ''')
        print("write sucessfull.")
except Exception as e:
    print(f"Error while write/append,{e}")