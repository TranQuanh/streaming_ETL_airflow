﻿# STREAMING DATAPIPELINE [GLAMIRA](www.glamira.com) BEHAVIOUR
<!-- Start Document Outline -->
* Batch data pipeline
  * [👜Business Case and Requirement](business-case-and-requirement)
  * [💻Step by step this project](#step-by-step)
    * [Architecture](#architecture)
      * [ETL flow](#etl-flow)
    * [Setup docker](#setup-docker)
        * [Kafka](#kafka)
        * [Spark](#data-transform)
        * [Postgres](#postgres)
        * [Airflow](#airflow)
    * [How to run](#how-to-run)
      * [Data ingestion](#data-ingestion)
      * [Data transform](#data-transform)
      * [Data load](#data-ingestion)
--
## Business Case and Requirement
### 🔳Business case
In this project, we use raw data(behaviour user) in [www.glamira.com](www.glamira.com) transform into a more accessible format for extracting insights.
Use Apache Airflow to create DAGs for:

- Regularly monitor the Kafka server to ensure data is flowing from the server to the local Kafka setup.
- Pull data periodically from the remote Kafka server to the local
- Configure the DAG to launch the Spark Streaming  job whenever new data is detected in Kafka
- After Spark processes the data, add an **Airflow task** to verify the quality of the processed data.
- Schedule Airflow tasks to archive processed data to a long-term storage solution (*)
- Set up task to send alerts for issues in any part of the pipeline, such as:
    - Data ingestion failure from the Kafka server.
    - Spark job failure.
    - Quality check failure in the processed data.
### Presiquites
In this project, we'll use kafka to like source data where we ingest from, spark to transform data and postgres to locate datawarehouse and use Airflow to montitor our work, if it go down, some message will be sent to our email.
## 💻Step by step this project
### Architecture
The pipeline take data from [www.glamira.com](www.glamira.com) and transform into insight data
 - **Kafka**:  It's designed for high-throughput, fault-tolerant, and scalable real-time data streams
 - **spark streaming** :designed for processing real-time data streams using micro batching.
 -  **airflow** :  is an open-source platform to programmatically author, schedule, and monitor workflows.
### Setup docker
#### Kafka
 - Run docker compspose
```shell
docker compose up -d
```
use localhost:8180 to monitor kafka topic by akhq

#### Spark

Firstly, build a custom image using Dockerfile

```shell
docker build -t unigap/spark:3.5 .
```

Then creating `spark_data` and `spark_lib` volume

```shell
docker volume create spark_data
docker volume create spark_lib
```

Start spark using compose file

```shell
docker compose up -d
```
localhost:8080 to manage spark worker
### Postgres
run docker compose
```
docker compose up -d
```
localhost:8380 to manage database
### Airflow
run docker compose
```
docker compose up -d
```
localhost:8090 to monitor airflow
## How to run
### Data ingestion
**this is what  data look like**
```
_id:5ed8cb2bc671fc36b74653ad
time_stamp:1591266092
ip:"37.170.17.183"
user_agent:"Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/6…"
resolution:"375x667"
user_id_db:"502567"
device_id:"beb2cacb-20af-4f05-9c03-c98e54a1b71a"
api_version:"1.0"
store_id:"12"
local_time:"2020-06-04 12:21:27"
show_recommendation:"false"
current_url:"https://www.glamira.fr/glamira-pendant-viktor.html?alloy=yellow-375"
referrer_url:"https://www.glamira.fr/men-s-necklaces/"
email_address:"pereira.vivien@yahoo.fr"
recommendation:false
utm_source:false
utm_medium:false
collection:"view_product_detail"
product_id:"110474"

option
Array (2)
0:Object
option_label:"alloy"
option_id:"332084"
value_label:""
value_id:"3279318"
1:Object
option_label:"diamond"
option_id:""
value_label:""
value_id:""
```
**data schema:**

| name          | tpye  | description                                                   |Example                                                                                                                                                              |
|--------------|---------------|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id           | String        | Log id                                                  | aea4b823-c5c6-485e-8b3b-6182a7c4ecce                                                                                                                                |
| api_version  | String        | Version của api                                         | 1.0                                                                                                                                                                 | 
| collection   | String        | Loại log                                                | view_product_detail                                                                                                                                                 | 
| current_url  | String        | Url của trang web mà người dùng đang vào                | https://www.glamira.cl/glamira-anillo-saphira-skug100335.html?alloy=white-375&diamond=sapphire&stone2=diamond-Brillant&itm_source=recommendation&itm_medium=sorting |
| device_id    | String        | id của thiết bị                                         | 874db849-68a6-4e99-bcac-fb6334d0ec80                                                                                                                                |
| email        | String        | Email của người dùng                                    |                                                                                                                                                                     |
| ip           | String        | Địa chỉ ip                                              | 190.163.166.122                                                                                                                                                     |
| local_time   | String        | Thời gian log được tạo. Format dạng yyyy-MM-dd HH:mm:ss | 2024-05-28 08:31:22                                                                                                                                                 |
| option       | Array<Object> | Danh sách các option của sản phẩm                       | `[{"option_id": "328026", "option_label": "diamond"}]`                                                                                                              |
| product_id   | String        | Mã id của sản phẩm                                      | 96672                                                                                                                                                               |
| referrer_url | String        | Đường dẫn web dẫn đến link `current_url`                | https://www.google.com/                                                                                                                                             |
| store_id     | String        | Mã id của cửa hàng                                      | 85                                                                                                                                                                  |
| time_stamp   | Long          | Timestamp thời điểm bản ghi log được tạo                |                                                                                                                                                                     |
| user_agent   | String        | Thông tin của browser, thiết bị                         | Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1                           |

Put all data into topic *spark* in kafka
### Data transform
- Use spark streaming to transform micro batch data
### Data load
- Load data into postgres like datawarehouse(fact and dimension)

***All process is manage by Airflow, so you just go to airflow and run dag, all process will be executed***
