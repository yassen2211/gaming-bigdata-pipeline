import os
from pyspark.sql import SparkSession
 
os.environ["HADOOP_USER_NAME"] = "root"
 
spark = SparkSession.builder \
    .appName('GamingETL_Load') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.29.96.1") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()
 
# ==============================
# Snowflake Settings
# ==============================
sf_options = {
    "sfURL":       "inhftrp-lj35534.snowflakecomputing.com",
    "sfUser":      "YASSENAMR1111",
    "sfPassword":  "YassenAmr1234567890?",
    "sfDatabase":  "GAMING_DB",
    "sfSchema":    "GOLD_LAYER",
    "sfWarehouse": "GAMING_WH"
}
 
GOLD_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"
 
def load_to_snowflake(table_name, mode="overwrite"):
    print(f" Loading {table_name} to Snowflake...")
    df = spark.read.parquet(f"{GOLD_PATH}{table_name}")
    print(f"   Rows to load: {df.count()}")
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name.upper()) \
        .mode(mode) \
        .save()
    print(f" {table_name} loaded successfully!")
 
try:
    load_to_snowflake("dim_games",      mode="overwrite")
    load_to_snowflake("dim_tags",       mode="overwrite")
    load_to_snowflake("fact_engagement", mode="overwrite")
 
    print("\n ALL TABLES LOADED TO SNOWFLAKE SUCCESSFULLY!")
 
except Exception as e:
    print(f" Loading failed: {e}")
    raise e
finally:
    spark.stop()
    print(" Spark Session Stopped.")
 
