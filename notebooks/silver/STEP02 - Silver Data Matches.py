# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.dbutils import *
from datetime import datetime, date

# COMMAND ----------

## variables

database = "ipl_database"
bronze = "bronze"
silver = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a table that will has all the match ids, venue, winner, toss winner, wheather a match was day/night, match date, teams etc.

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,batsman from ipl_database.bronze.batsman_data_raw
# MAGIC group by 1,2
# MAGIC having count(*) > 1
# MAGIC order by id

# COMMAND ----------

# MAGIC %skip
# MAGIC df = spark.readStream.table("ipl_database.bronze.batsman_data_raw") \
# MAGIC     .select(
# MAGIC         "id",
# MAGIC         "batting_team",
# MAGIC         "bowling_team",
# MAGIC         "date",
# MAGIC         "venue",
# MAGIC         "winner",
# MAGIC         "toss",
# MAGIC         "file_name"
# MAGIC     )

# COMMAND ----------

## get todays dates to process only new data

today_date = date.today()
print(today_date)

# COMMAND ----------

## batting raw data
df = spark.table(f"{database}.{bronze}.batsman_data_raw") \
    .filter(
        to_date(col("ingestion_time")) >= today_date
    ) \
    .select(
        "id",
        "batting_team",
        "bowling_team",
        "date",
        "venue",
        "winner",
        "toss",
        "file_name",
    ) 

# COMMAND ----------

# MAGIC %skip
# MAGIC df.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 00 Drop duplicates

# COMMAND ----------

df_dedup = df.dropDuplicates(['id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 Assigining UUID to match_id

# COMMAND ----------

df_uuid = (
  df_dedup.withColumn(
    "match_UUID",
    substring(sha2(concat_ws("-","id","batting_team","bowling_team","date"), 256),1,36)
  )
)

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql.functions import sha2, concat_ws
# MAGIC df_test = df_dedup.withColumn(
# MAGIC     "test_uuid",
# MAGIC     substring(sha2(concat_ws("-","id","batting_team","bowling_team","date"), 256),1,36)
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC df_test.display(10)

# COMMAND ----------

# MAGIC %skip
# MAGIC df_uuid.display(10)

# COMMAND ----------

## Check dist uuid are generated for each match id

df_uuid.groupBy("match_UUID").agg(
    count("*").alias("counts")
).filter(col("counts") > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 is_day 
# MAGIC  - 1 day match
# MAGIC  - 0 night match

# COMMAND ----------

df2 = df_uuid.withColumn(
  "is_day",
  when(regexp_extract("id",r'\((.*?)\)',1) == 'D/N',1).otherwise(0)
)

# COMMAND ----------

# MAGIC %skip
# MAGIC df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 toss winner

# COMMAND ----------

df3 = df2.withColumn(
  "toss_winner",
  trim(split(col("toss"),",")[0])
) \
.withColumn(
  "toss_decision",
  trim(split(col("toss"),",")[1])
) 
# df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 match winner

# COMMAND ----------

teams_abbrivations = [
  {
    "team_name": "Chennai Super Kings",
    "short_name": "CSK"
  },
  {
    "team_name": "Mumbai Indians",
    "short_name": "MI"
  },
  {
    "team_name": "Royal Challengers Bangalore",
    "short_name": "RCB"
  },
  {
    "team_name": "Kolkata Knight Riders",
    "short_name": "KKR"
  },
  {
    "team_name": "Delhi Capitals",
    "short_name": "DC"
  },
  {
    "team_name": "Punjab Kings",
    "short_name": "PBKS"
  },
  {
    "team_name": "Rajasthan Royals",
    "short_name": "RR"
  },
  {
    "team_name": "Sunrisers Hyderabad",
    "short_name": "SRH"
  },
  {
    "team_name": "Lucknow Super Giants",
    "short_name": "LSG"
  },
  {
    "team_name": "Gujarat Titans",
    "short_name": "GT"
  }
]

# COMMAND ----------

match_abbrivation_df = spark.createDataFrame(teams_abbrivations)
# match_abbrivation_df.display()

# COMMAND ----------

from pyspark.sql import Window
window_spec = Window.orderBy(col("match_date"),col("id"))

df4 = df3.withColumn(
    "match_winner",
    trim(split(col("winner")," ")[0])
) \
.withColumn(
    "match_date",
    to_date(trim(col("date")), "MMMM dd yyyy")
).alias("a") \
.join(
    match_abbrivation_df.alias("b"),
    on = col("a.match_winner") == col("b.short_name"),
    how="left"
) \
.withColumn(
    "match_rank",
    rank().over(window_spec)
) \
.orderBy(col("match_date"),col("id"))
# df4.display()

# COMMAND ----------

## get max match_id for the particular season
md_silver = spark.table("ipl_database.silver.match_details")
max_id = md_silver.groupBy("season").agg(
    max("match_number").alias("max_match_id")
).distinct()
max_id.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 resolve date and venue and rename columns names

# COMMAND ----------

df6 = (
  df4
  .withColumn(
    "venue",
    trim(col("venue"))
  )
  .withColumn(
    "team_1",
    trim(col("batting_team"))
  )
  .withColumn(
    "team_2",
    trim(col("bowling_team"))
  )
  .withColumn(
    "season",
    year(col("match_date"))
  )
  .join(
    max_id,
    on="season",
    how="left"
  )
  .withColumn(
    "match_number",
    col("match_rank") + coalesce(col("max_match_id"),lit(0))
  )
)
# df6.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final table

# COMMAND ----------

df_fin = df6.select(
    "match_UUID",
    "match_number",
    "match_date",
    "id",
    "team_1",
    "team_2",
    "venue",
    "toss_winner",
    "toss_decision",
    col("team_name").alias("match_winner"),
    "is_day",
    "season"
).orderBy(col("id"))

# COMMAND ----------

# MAGIC %skip
# MAGIC df_fin.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving delta table

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

file_location = "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta"

# COMMAND ----------

# MAGIC %skip
# MAGIC ## overwrite or append
# MAGIC
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC if not DeltaTable.isDeltaTable(spark,file_location):
# MAGIC     df_fin.writeStream \
# MAGIC         .format("delta") \
# MAGIC         .option("checkpointLocation","abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/checkpoints/") \
# MAGIC         .mode("overwrite") \
# MAGIC         .option("mergeschema","true") \
# MAGIC         .trigger(availableNow=True) \
# MAGIC         .start("abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/")
# MAGIC     spark.sql(f'''
# MAGIC               create table ipl_database.silver.match_details
# MAGIC               using delta
# MAGIC               location 'abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/'
# MAGIC               ''')
# MAGIC else:
# MAGIC     ## upsert ops
# MAGIC     df = spark.table(f"ipl_database.silver.match_details")
# MAGIC     df_fin.writeStream \
# MAGIC         .format("delta") \
# MAGIC         .option("checkpointLocation","abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/checkpoints/") \
# MAGIC         .option("mergeSchema","true") \
# MAGIC         .merge(
# MAGIC             source = df_fin.alias("updates"),
# MAGIC             target = df.alias("match_details")
# MAGIC         ) \
# MAGIC         .whenMatchedUpdateAll() \
# MAGIC         .whenNotMatchedInsertAll() \
# MAGIC         .trigger(availableNow=True) \
# MAGIC         .start("abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/")

# COMMAND ----------

# MAGIC %skip
# MAGIC ## processed files 
# MAGIC try:
# MAGIC     new_data = df_fin.select("match_number","season").distinct()
# MAGIC     new_data.createOrReplaceTempView("new_data")
# MAGIC     spark.sql(f'''
# MAGIC               merge into ipl_database.silver.match_control mc
# MAGIC               using new_data nd
# MAGIC               on mc.match_number = nd.match_number and mc.season = nd.season
# MAGIC               when matched then update set *
# MAGIC               when not matched then insert *
# MAGIC               ''')
# MAGIC     print("append sucessfull.")
# MAGIC except:
# MAGIC     processed_files = df_fin.select("match_number","season").distinct()
# MAGIC     processed_files.write.format("delta") \
# MAGIC         .mode("overwrite") \
# MAGIC         .save(f"abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/control_table/")
# MAGIC     spark.sql(f'''
# MAGIC               create table ipl_database.silver.match_control
# MAGIC               using delta
# MAGIC               location "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/control_table/"
# MAGIC               ''')
# MAGIC     print("write sucessfull.")

# COMMAND ----------

# MAGIC %skip
# MAGIC df_fin.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC delta_tables = DeltaTable.forPath(spark, "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/")
# MAGIC
# MAGIC history_df = delta_tables.history()
# MAGIC history_df.display()

# COMMAND ----------

# delta_tables.restoreToVersion(3)

# COMMAND ----------

# spark.sql('refresh table ipl_database.silver.match_details')

# COMMAND ----------

try:
    if spark.catalog.tableExists("ipl_database.silver.match_details"):
        df_fin.createOrReplaceTempView("df_fin")
        spark.sql(f'''
                merge into ipl_database.silver.match_details mc
                using df_fin nd
                on mc.season = nd.season and mc.id = nd.id
                -- when matched then update set *
                when not matched then insert *
                ''')
        # Upsert in not ideal here, switching to append process
        print("append sucessfull.")
    else:
        df_fin.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/")
        spark.sql(f'''
                create table ipl_database.silver.match_details
                using delta
                location "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/"
                ''')
        print("write sucessfull.")
except Exception as e:
    print(f"Error while write/append,{e}")

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from ipl_database.silver.match_details

# COMMAND ----------

# MAGIC %skip
# MAGIC ## write final match details table
# MAGIC
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC if not DeltaTable.isDeltaTable(spark, "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/"):
# MAGIC     df_fin.write.mode("overwrite") \
# MAGIC     .format("delta") \
# MAGIC     .save("abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/") 
# MAGIC
# MAGIC
# MAGIC     spark.sql(f'''
# MAGIC                 create table ipl_database.silver.match_details
# MAGIC                 using delta
# MAGIC                 location "abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/"
# MAGIC                 ''')
# MAGIC     print("write sucess!")
# MAGIC else:
# MAGIC     df_fin.write.format("delta") \
# MAGIC         .mode("append") \
# MAGIC         .save("abfss://silver@ipldatastorageaccount.dfs.core.windows.net/match_details/delta/")
# MAGIC     print("append sucess")