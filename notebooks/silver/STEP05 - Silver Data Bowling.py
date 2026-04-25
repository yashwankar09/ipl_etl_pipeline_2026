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
checkpoint_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/bowling/checkpoints/'
schema_path = '/Volumes/ipl_database/bronze/ipl_pipeline_external_volume/bowling/schema/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the external table

# COMMAND ----------

df = spark.table("ipl_database.bronze.bowler_raw_data").filter(
    to_date(col("ingestion_time")) >= date.today()
)
# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 00 Extracting bowler stats

# COMMAND ----------

df = df.withColumn(
  "stats_array",
  from_json(
    col("stats"),
    ArrayType(StringType())
  )
)

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extracting details from stats array

# COMMAND ----------

df = (
  df
  .withColumn(
    "non_valid_deliveries",
    col("stats_array")[0].cast('int')
  )
  .withColumn(
    "wide_deliveries",
    col("stats_array")[1].cast('int')
  )
  .withColumn(
    "sixes_conceded",
    col("stats_array")[2].cast('int')
  )
  .withColumn(
    "fours_conceded",
    col("stats_array")[3].cast('int')
  )
  .withColumn(
    "dot_deliveries",
    col("stats_array")[4].cast('int')
  )
  .withColumn(
    "match_economy",
    col("stats_array")[5].cast('double')
  )
  .withColumn(
    "runs_conceded_in_match",
    col("stats_array")[6].cast('int')
  )
  .withColumn(
    "maiden_overs_in_match",
    col("stats_array")[7].cast('int')
  )
  .withColumn(
    "overs_bowled_in_match",
    col("stats_array")[8].cast('double')
  )
)

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 Adding IPL season

# COMMAND ----------

df = df.withColumn(
    "season",
    year(to_date(trim(col("date")),"MMMM dd yyyy"))
)
# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## trim leading and trailing spaces from the columns

# COMMAND ----------

df = (
  df
  .withColumn(
    "id",
    trim(col("id"))
  )
  .withColumn(
    "player_name",
    trim(col("bowler"))
  )
  .withColumn(
    "wickets_in_match",
    col("wicket").cast('int')
  )
)

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating final silver table

# COMMAND ----------

# MAGIC %skip
# MAGIC df.columns

# COMMAND ----------

df_fin = df.select(
    'id',
    'player_name',
    'wickets_in_match',
    'runs_conceded_in_match',
    'non_valid_deliveries',
    'wide_deliveries',
    'sixes_conceded',
    'fours_conceded',
    'dot_deliveries',
    'match_economy',
    'maiden_overs_in_match',
    'overs_bowled_in_match',
    'season',
)

# COMMAND ----------

# df_fin.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ++ Match UUID

# COMMAND ----------

match_uuid = spark.table("ipl_database.silver.match_details")

df_fin1 = df_fin.alias('a') \
  .join(
    match_uuid.alias('b'),
    on=col('a.id') == col('b.id'),
    how='left'
  ) \
  .drop('id') \
  .select('a.*','b.match_UUID')
# df_fin1.display()

# COMMAND ----------

try:
    if spark.catalog.tableExists("ipl_database.silver.bowler_stats"):
        df_fin1.createOrReplaceTempView('df_fin1')
        spark.sql('''
                  merge into ipl_database.silver.bowler_stats bs
                  using df_fin1 df
                  on bs.season = df.season and bs.match_UUID = df.match_UUID and bs.player_name = df.player_name
                --   when matched then update set *
                  when not matched then insert *
                  ''')
        # df_fin1.write.format("delta").mode("append").save(f"abfss://silver@ipldatastorageaccount.dfs.core.windows.net/bowling/")
        spark.sql('''
                refresh table ipl_database.silver.bowler_stats
                ''')
        print("data inserted")
    else:
        df_fin1.write.format("delta").mode("overwrite").save(f"abfss://silver@ipldatastorageaccount.dfs.core.windows.net/bowling/")
        spark.sql('''
                create table ipl_database.silver.bowler_stats
                using delta
                location "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/bowling/"
                ''')
        print("Data written!")
except Exception as e:
    print(f"Error while write/append {e}")