import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
 
os.environ["HADOOP_USER_NAME"] = "root"
 
spark = SparkSession.builder \
    .appName('GamingETL_Transform') \
    .master('local[*]') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.29.96.1") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()
 
BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/online_gaming/"
GOLD_PATH   = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"
 
try:
    print(" Reading Bronze Layer...")
    df = spark.read.parquet(BRONZE_PATH)
    print(f" Loaded {df.count()} records")
 
    # ==============================
    # تنضيف الداتا
    # ==============================
    print(" Cleaning data...")
 
    # امسح الصفوف اللي name فيها null
    df = df.dropna(subset=["name"])
 
    # امسح الـ duplicates
    df = df.dropDuplicates(["name"])
 
    # ملي الـ nulls في description و tags
    df = df.fillna({"description": "No description available", "tags": "uncategorized"})
 
    # نضف الـ name من spaces زيادة
    df = df.withColumn("name", F.trim(F.col("name")))
 
    # حساب engagement_rate
    df = df.withColumn(
        "total_votes",
        F.col("likes") + F.col("dislikes")
    ).withColumn(
        "engagement_rate",
        F.when(
            F.col("total_votes") > 0,
            F.round(F.col("likes") / F.col("total_votes") * 100, 2)
        ).otherwise(0.0)
    )
 
    # تصنيف اللعبة حسب الـ engagement
    df = df.withColumn(
        "popularity_tier",
        F.when(F.col("engagement_rate") >= 80, "Top")
         .when(F.col("engagement_rate") >= 60, "Good")
         .when(F.col("engagement_rate") >= 40, "Average")
         .otherwise("Low")
    )
 
    print(f" After cleaning: {df.count()} records")
 
    # ==============================
    # Dim Games
    # ==============================
    print(" Creating dim_games...")
 
    dim_games = df.select(
        F.monotonically_increasing_id().alias("game_key"),
        F.col("name").alias("game_name"),
        F.col("url").alias("game_url"),
        F.col("description")
    )
 
    dim_games.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_games")
    print(" dim_games created")
 
    # ==============================
    # Dim Tags
    # ==============================
    print(" Creating dim_tags...")
 
    dim_tags = df.select(
        F.col("name").alias("game_name"),
        F.explode(F.split(F.col("tags"), ",")).alias("tag")
    ).withColumn("tag", F.trim(F.col("tag"))) \
     .filter(F.col("tag") != "") \
     .distinct() \
     .withColumn("tag_key", F.monotonically_increasing_id())
 
    dim_tags.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_tags")
    print(" dim_tags created")
 
    # ==============================
    # Fact Engagement
    # ==============================
    print(" Creating fact_engagement...")
 
    fact_engagement = df.select(
        F.monotonically_increasing_id().alias("fact_key"),
        F.col("name").alias("game_name"),
        F.col("likes"),
        F.col("dislikes"),
        F.col("total_votes"),
        F.col("engagement_rate"),
        F.col("popularity_tier"),
        F.col("log_likes"),
        F.col("log_dislikes")
    )
 
    fact_engagement.write.mode("overwrite").parquet(f"{GOLD_PATH}fact_engagement")
    print(" fact_engagement created")
 
    # ==============================
    # Summary
    # ==============================
    print("\n Summary:")
    print(f"   dim_games:      {dim_games.count()} rows")
    print(f"   dim_tags:       {dim_tags.count()} rows")
    print(f"   fact_engagement:{fact_engagement.count()} rows")
 
    print("\n Transform complete! Gold Layer is ready.")
 
except Exception as e:
    print(f" Error: {e}")
    raise e
finally:
    spark.stop()
    print(" Spark Session Stopped.")
 
