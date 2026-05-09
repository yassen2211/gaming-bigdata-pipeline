import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
os.environ["HADOOP_USER_NAME"] = "root"
 
spark = SparkSession.builder \
    .appName('GamingETL_Extract') \
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
 
print(" Spark Connected Successfully")
 
schema = StructType([
    StructField("name",         StringType(),  True),
    StructField("url",          StringType(),  True),
    StructField("likes",        IntegerType(), True),
    StructField("dislikes",     IntegerType(), True),
    StructField("log_likes",    DoubleType(),  True),
    StructField("log_dislikes", DoubleType(),  True),
    StructField("description",  StringType(),  True),
    StructField("tags",         StringType(),  True),
])
 
input_path = "hdfs://hadoop-namenode:9000/raw/online_gaming/"
print(f" Reading from HDFS: {input_path}")
 
try:
    raw_df = spark.read \
        .schema(schema) \
        .option("header", "true") \
        .csv(input_path)
 
    record_count = raw_df.count()
    print(f" Found {record_count} records")
 
    if record_count > 0:
        bronze_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/online_gaming/"
 
        print(f" Writing to Bronze Layer: {bronze_path}")
 
        raw_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(bronze_path)
 
        print(" Extract complete. Data saved as Parquet in Bronze Layer.")
        raw_df.printSchema()
        raw_df.show(5, truncate=True)
    else:
        print(" No data found in HDFS!")
 
except Exception as e:
    print(f"❌ Error: {e}")
    raise e
finally:
    spark.stop()
    print("🛑 Spark Session Stopped.")
