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

# COMMAND ----------

## batting_raw

df_bat_raw = spark.table('ipl_database.bronze.batsman_data_raw').select("batsman","batting_team","date").distinct()
df_ball_raw = spark.table('ipl_database.bronze.bowler_raw_data').select("bowler","bowling_team","date").distinct()

# COMMAND ----------

# MAGIC %skip
# MAGIC # df_bat_raw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 00 Batsman clean batsman name column and trim for leading and trailing blank spaces

# COMMAND ----------

df_bat_raw = df_bat_raw.withColumn(
    "player_name",
    regexp_replace(col("batsman"),r'\(c\)',"")
) \
.withColumn(
    "player_name",
    regexp_replace(col("player_name"),"�","")
) \
.withColumn(
    "captain",
    when(col("batsman").contains("(c)"),col("player_name")).otherwise("-")
) \
.withColumn(
    "team",
    trim(col("batting_team"))
) \
.withColumn(
    "role",
    lit("Batsman")
) \
.withColumn(
    "season",
    year(to_date(trim("date"),"MMMM dd yyyy"))
).select("team","player_name","role","season")

# COMMAND ----------

# MAGIC %skip
# MAGIC df_bat_raw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 Bowler raw data

# COMMAND ----------

df_ball_raw = df_ball_raw.withColumn(
    "team",
    trim(col("bowling_team"))
) \
.withColumn(
    "player_name",
    trim(col("bowler"))
) \
.withColumn(
    "season",
    year(to_date(trim("date"),"MMMM dd yyyy"))
) \
.withColumn(
    "role",
    lit("Bowler")
).select("team","player_name","role","season")

# COMMAND ----------

# MAGIC %skip
# MAGIC df_ball_raw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 Append bowler and batsman data

# COMMAND ----------

df_ball_raw.createOrReplaceTempView("df_ball_raw")
df_bat_raw.createOrReplaceTempView("df_bat_raw")

# COMMAND ----------

remove_bowlers = df_bat_raw.alias('a') \
    .join(
        df_ball_raw.alias('b'),
        (col('a.player_name') == col('b.player_name')) & (col('a.season') == col('b.season')),
        'leftanti'
    ) \
    .select('a.*')

# COMMAND ----------

# MAGIC %skip
# MAGIC remove_bowlers.display()

# COMMAND ----------

df_fin = remove_bowlers.unionAll(df_ball_raw).orderBy("team").distinct()
# df_fin.display()

# COMMAND ----------

try:
    if spark.catalog.tableExists("ipl_database.silver.player_details"):
        df_fin.createOrReplaceTempView('df_fin')
        spark.sql('''
                merge into ipl_database.silver.player_details pd
                using df_fin df
                on pd.team = df.team and pd.player_name = df.player_name and pd.season = df.season
                -- when matched then update set *
                when not matched then insert *
                ''')
        print("Data Inserted Sucessfully!")
    else:
        df_fin.write.format("delta").mode("overwrite").save(f"abfss://silver@ipldatastorageaccount.dfs.core.windows.net/player_details/")
        spark.sql('''
                create table ipl_database.silver.player_details
                using delta
                location "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/player_details/"
                ''')
        print("Data Written Sucessfully!")

except Exception as e:
    print(f"Error while write/append {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 finding all rounder
# MAGIC  - batsman who never bowled will be pure batters else they will be marked all rounders

# COMMAND ----------

# MAGIC %skip
# MAGIC df_allrounder = df_bat_raw.alias('a') \
# MAGIC     .join(
# MAGIC         df_ball_raw.alias('b'),
# MAGIC         (col('a.player_name') == col('b.player_name')) & (col('a.season') == col('b.season')),
# MAGIC         'left'
# MAGIC     ) \
# MAGIC     .withColumn(
# MAGIC         'is_allrounder',
# MAGIC         when(col('a.player_name') == col('b.player_name'),lit(1)).otherwise(lit(0))
# MAGIC     ) \
# MAGIC     .select("a.team","a.player_name","a.role","a.season","is_allrounder")
# MAGIC df_allrounder.display()