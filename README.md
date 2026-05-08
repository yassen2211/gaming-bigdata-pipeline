\# 🎮 Gaming Big Data Pipeline



A complete Big Data Pipeline project using:



\- Apache Airflow

\- Apache Spark

\- Hadoop HDFS

\- Snowflake

\- Docker

\- Python



\---



\# 📌 Project Overview



This project simulates a real-world gaming analytics pipeline.



The pipeline performs:



1\. Upload raw gaming transaction data to HDFS

2\. Extract data using Spark

3\. Transform data into analytical models

4\. Load processed data into Snowflake

5\. Automate everything using Apache Airflow



\---



\# 🏗️ Architecture



Gaming CSV Data  

↓  

Apache Airflow  

↓  

HDFS (Raw Layer)  

↓  

Apache Spark  

↓  

Processed Data  

↓  

Snowflake Data Warehouse



\---



\# ⚙️ Technologies Used



| Technology | Purpose |

|---|---|

| Apache Airflow | Workflow Orchestration |

| Apache Spark | Big Data Processing |

| Hadoop HDFS | Distributed Storage |

| Snowflake | Cloud Data Warehouse |

| Docker | Containerization |

| Python | Pipeline Development |



\---



\# 📂 Project Structure



```bash

gaming\_pipeline/

│

├── dags/

│   └── gaming\_pipeline\_dag.py

│

├── 1\_upload\_to\_hdfs.py

├── 2\_extract.py

├── 3\_transform.py

├── 4\_load\_to\_snowflake.py

│

├── docker-compose.yml

├── README.md

└── .gitignore







How To Run

1️⃣ Start Docker Containers

docker-compose up -d

2️⃣ Open Airflow

http://localhost:8080



Default Login:

Username: airflow

Password: airflow



🔄 Airflow Pipeline



The DAG automates:

Uploading CSV batches to HDFS

Running Spark extraction

Running Spark transformations

Loading final tables into Snowflake



📊 Data Flow

Raw CSV → HDFS → Spark Processing → Snowflake



✅ Features

Automated ETL Pipeline

Distributed Data Processing

Cloud Data Warehouse Integration

Scalable Architecture

Dockerized Environment



👨‍💻 Author

Yassen Amr



---

# 🗄️ DWH Schema Diagram

## ⭐ Star Schema Design

### Fact Table
- fact_game_transactions

### Dimension Tables
- dim_player
- dim_game
- dim_time
- dim_location

---

## 📊 Schema Structure

                dim_player
                     |
                     |
dim_game ---- fact_game_transactions ---- dim_time
                     |
                     |
               dim_location

---

## 📌 Fact Table Columns

| Column | Description |
|---|---|
| transaction_id | Unique transaction ID |
| player_id | Player identifier |
| game_id | Game identifier |
| time_id | Time identifier |
| location_id | Location identifier |
| amount | Transaction amount |

---

## 📌 Dimension Tables

### dim_player
- player_id
- player_name
- age
- country

### dim_game
- game_id
- game_name
- category

### dim_time
- time_id
- date
- month
- year

### dim_location
- location_id
- city
- country

