import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col,hash,abs,lower
from pyspark.sql.types import StructType, StructField, StringType,IntegerType,LongType
from pyspark.sql.functions import col, date_format, hour, dayofmonth, month, year, lit
from util.config import Config
from util.logger import Log4j
# conf = Config()
# spark_conf = conf.spark_conf
# kafka_conf = conf.kafka_conf

# spark = SparkSession \
#         .builder \
#         .config(conf=spark_conf) \
#         .getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")


def create_dim_product(spark):
    # Định nghĩa schema cho product_name
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False)
    ])
    # Đọc dữ liệu từ file JSON
    with open('/data/project/data/json_product_name.json', 'r') as file:
        data = json.load(file)

    # Chuyển đổi dữ liệu thành danh sách các tuple (key, value)
    data_list = [(int(k), v) for k, v in data.items()]
    # Chuyển đổi dictionary thành danh sách tuple
    country_df = spark.createDataFrame(data_list, schema=schema)
    return country_df
# country_df.show()
def create_dim_territory(spark):
    # Thiết kế StructType
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("alpha-2", StringType(), True),
        StructField("alpha-3", StringType(), True),
        StructField("country-code", IntegerType(), True),
        StructField("iso_3166-2", StringType(), True),
        StructField("region", StringType(), True),
        StructField("sub-region", StringType(), True),
        StructField("intermediate-region", StringType(), True),
        StructField("region-code", IntegerType(), True),
        StructField("sub-region-code", IntegerType(), True),
        StructField("intermediate-region-code", IntegerType(), True)
    ])
    territory_df = spark.read.csv('/data/project/data/country_code.csv',schema = schema,header = True)
    territory_key = abs(hash(lower(col('alpha-2'))))
    territory_df = territory_df.withColumn('territory_id', territory_key)
    territory_df = territory_df.select(
                col('territory_id'),
                col('alpha-2').alias('country_code'),
                col('name').alias('country_name'),
                col('iso_3166-2').alias('iso_3166_2'),
                col('region'),
                col('sub-region').alias('sub_region'),
                col('intermediate-region').alias('intermediate_region')
                )
    return territory_df
# product_df.show()
def create_dim_date(spark):
    date_df = spark.sql("""
    SELECT sequence(to_timestamp('2020-01-01 00:00:00'), to_timestamp('2020-12-31 23:00:00'), interval 1 hour) AS full_date
    """).selectExpr("explode(full_date) as full_date")

    # Thêm các cột cần thiết
    date_df = date_df.withColumn("date_id", date_format(col("full_date"), "HHddMMyyyy").cast(LongType())) \
        .withColumn("day_of_week", date_format(col("full_date"), "EEEE")) \
        .withColumn("day_of_week_short", date_format(col("full_date"), "E")) \
        .withColumn("day_of_month", dayofmonth(col("full_date"))) \
        .withColumn("month", month(col("full_date"))) \
        .withColumn("year", year(col("full_date"))) \
        .withColumn("hour", hour(col("full_date")))
    date_df = date_df.select("date_id", "full_date", "day_of_week", "day_of_week_short", "day_of_month", "year", "month", "hour")
    return date_df
# spark.stop()