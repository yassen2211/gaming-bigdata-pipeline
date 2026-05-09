import pandas as pd
import requests
import math
import os
 
# ==============================
# Settings
# ==============================
CSV_FILE = "/tmp/online_gaming.csv"
HDFS_HOST = "http://hadoop-namenode:9870"
HDFS_RAW_DIR = "/raw/online_gaming"
BATCH_SIZE = 500
 
# ==============================
# Step 1: قرا الداتا
# ==============================
print(" Loading data...")
df = pd.read_csv(CSV_FILE, sep='\t', on_bad_lines='skip')
print(f" Loaded {len(df)} rows and {len(df.columns)} columns")
print(f" Columns: {df.columns.tolist()}")
 
# ==============================
# Step 2: عمل HDFS directory
# ==============================
print(" Creating HDFS directory...")
url = f"{HDFS_HOST}/webhdfs/v1{HDFS_RAW_DIR}?op=MKDIRS&user.name=root"
requests.put(url)
print(" Directory created")
 
# ==============================
# Step 3: قسّم الداتا لـ Batches
# ==============================
total_batches = math.ceil(len(df) / BATCH_SIZE)
print(f" Total batches: {total_batches}")
 
for i in range(total_batches):
    start = i * BATCH_SIZE
    end = start + BATCH_SIZE
    batch = df[start:end]
 
    # احفظ الـ batch مؤقتاً
    local_path = f"/tmp/gaming_batch_{i+1}.csv"
    batch.to_csv(local_path, index=False)
 
    # ابعته لـ HDFS عن طريق WebHDFS
    hdfs_path = f"{HDFS_RAW_DIR}/batch_{i+1}.csv"
 
    # Step 1: اطلب الـ redirect
    url = f"{HDFS_HOST}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
    r = requests.put(url, allow_redirects=False)
 
    # Step 2: ابعت الداتا
    redirect_url = r.headers['Location']
    with open(local_path, 'rb') as f:
        requests.put(redirect_url, data=f)
 
    # امسح الـ temp file
    os.remove(local_path)
 
    print(f" Batch {i+1}/{total_batches} uploaded to HDFS")
 
print(" All batches uploaded to HDFS successfully!")
 
