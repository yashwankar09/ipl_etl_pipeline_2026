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

bronze_layer = "bronze"
silver_layer = "silver"

target_schema = "IPL_database"
bronze_schema = "bronze"
silver_schema = "silver"

# COMMAND ----------

df = spark.table(f"{target_schema}.{bronze_schema}.batsman_data_raw").filter(
    to_date(col("ingestion_time")) >= date.today()
)

# COMMAND ----------

# MAGIC %skip
# MAGIC df.columns

# COMMAND ----------

# MAGIC %skip
# MAGIC df = df.select(
# MAGIC     col("id").cast("string"),
# MAGIC     col("batsman").cast("string"),
# MAGIC     col("wicket_by").cast("string"),
# MAGIC     col("stats").cast("string"),
# MAGIC     col("batting_team").cast("string"),
# MAGIC     col("bowling_team").cast("string"),
# MAGIC     col("date").cast("string"),
# MAGIC     col("venue").cast("string"),
# MAGIC     col("winner").cast("string"),
# MAGIC     col("toss").cast("string"),
# MAGIC     col("runs_scored").cast("string"),
# MAGIC     col("_rescued_data").cast("string"),
# MAGIC     col("file_name").cast("string"),
# MAGIC     col("ingestion_time").cast("timestamp")
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 Cleaning batsman name column

# COMMAND ----------

# MAGIC %skip
# MAGIC df.select("batsman").distinct().display()

# COMMAND ----------

df = df.withColumn(
    "is_captain",
    when(col("batsman").contains("(c)"),lit(1)).otherwise(lit(0))
) \
.withColumn(
    "player_name",
    regexp_replace(col("batsman"),r'\(c\)',"")
) \
.withColumn(
    "player_name",
    regexp_replace(col("player_name"),"�","")
) \
.withColumn(
    "wicket_by",
    regexp_replace(col("wicket_by"),"�","")
)

# df.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 Deal with stats column

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
# MAGIC ### Create new columns from stats column
# MAGIC
# MAGIC mapping is 
# MAGIC
# MAGIC - 0 : balls played by batsman
# MAGIC - 1 : minutes played
# MAGIC - 2 : 4's
# MAGIC - 3 : 6's
# MAGIC - 4 : String rate

# COMMAND ----------

def safe_convert(x):
    try:
        return int(x)
    except:
        return 0
    
# register a UDF
safe_udf = udf(safe_convert,IntegerType())

# COMMAND ----------

df = (
    df.withColumn(
        "balls_played",
        safe_udf(col("stats_array")[0])
    )
    .withColumn(
        "mintues_batted",
        safe_udf(col("stats_array")[1])
    )
    .withColumn(
        "boundries",
        safe_udf(col("stats_array")[2])
    )
    .withColumn(
        "sixes",
        safe_udf(col("stats_array")[3])
    )
    .withColumn(
        "Strike_rate",
        (when(col("stats_array")[4] == '-',lit("0")).otherwise(col("stats_array")[4])).cast(DecimalType(10,2))
    )
)
# df.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df = (
# MAGIC     df.withColumn(
# MAGIC         "balls_played",
# MAGIC         safe_udf(col("stats_array")[0])
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "mintues_batted",
# MAGIC         col("stats_array")[1].cast("int")
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "boundries",
# MAGIC         col("stats_array")[2].cast("int")
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "sixes",
# MAGIC         col("stats_array")[3].cast("int")
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "Strike_rate",
# MAGIC         try:
# MAGIC             col("stats_array")[4].cast("double")
# MAGIC         except:
# MAGIC             lit(0)
# MAGIC     )
# MAGIC )
# MAGIC df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 Fix match date column

# COMMAND ----------

df = df.withColumn(
    "match_date",
    to_date(trim(col("date")),"MMMM dd yyyy")
)

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 Add derived columns
# MAGIC ++ is out, is_not_out, is_run_out

# COMMAND ----------

df = (
  df.withColumn(
    "wicket_by_player",
    trim(trim(col("wicket_by")))
  )
  .withColumn(
    "is_out",
    when((col("wicket_by_player")!="-") & (col("wicket_by_player").isNotNull()),lit(1)).otherwise(lit(0))
  )
  .withColumn(
    "is_not_out",
    when(col("wicket_by_player") == '-',lit(1)).otherwise(lit(0))
  )
  .withColumn(
    "is_run_out",
    when(col("wicket_by_player").contains("run out"),lit(1)).otherwise(lit(0))
  )

)

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 fix the columns with leading and trailing white spaces

# COMMAND ----------

## cols to be corrects

cols = ["id","venue","winner","batting_team","bowling_team","toss","player_name"]

# COMMAND ----------

df = df.select(
    *[trim(col(c)).alias(c) if c in cols else col(c) for c in df.columns]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06 convert runs scored to int

# COMMAND ----------

df = df.withColumn(
    "runs_scored",
    col("runs_scored").cast("int")
)

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match id

# COMMAND ----------

df = (
  df.withColumn(
    "season",
    year(col("match_date"))
  )
)
# df.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df.display()

# COMMAND ----------

## create final table

df_fin = df.select(
 'id',
 'player_name',
 'balls_played',
 'runs_scored',
 'mintues_batted',
 'boundries',
 'sixes',
 'Strike_rate',
 'wicket_by_player',
 'is_captain',
 'is_out',
 'is_not_out',
 'is_run_out',
 'season'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ++ match uuid

# COMMAND ----------

df_fin1 = df_fin.alias("a").join(
  spark.table(f"ipl_database.silver.match_details").alias("b"),
  on = "id",
  how = "inner"
).drop(col("id")).select("a.*","b.match_UUID")

# COMMAND ----------

df_fin1.filter(col('player_name') == 'Abhishek Sharma').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert

# COMMAND ----------

try:
    if spark.catalog.tableExists("ipl_database.silver.batsman_stats"):
        df_fin1.createOrReplaceTempView("df_fin1")
        spark.sql('''
                merge into ipl_database.silver.batsman_stats a
                using df_fin1 b
                on a.match_UUID=b.match_UUID and a.player_name=b.player_name and a.season=b.season
                -- when matched then update set *
                when not matched then insert *
                ''')
        # df_fin.write.format("delta").mode("append").save("abfss://silver@ipldatastorageaccount.dfs.core.windows.net/batting/")
        spark.sql('refresh table ipl_database.silver.batsman_stats')
        print("Data Inserted.")
    else:
        df_fin1.write.format("delta").mode("overwrite").save("abfss://silver@ipldatastorageaccount.dfs.core.windows.net/batting/")
        spark.sql(f"CREATE TABLE IF NOT EXISTS ipl_database.silver.batsman_stats USING DELTA LOCATION 'abfss://silver@ipldatastorageaccount.dfs.core.windows.net/batting/'")
        print("Table Created.")
except Exception as e:
    print(f"Error while write/append,{e}")