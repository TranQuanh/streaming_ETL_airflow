import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType, TimestampType,IntegerType
from user_agents import parse
from util.config import Config
from util.logger import Log4j
import postgres_database as db_ops
from dim_table import create_dim_date,create_dim_product,create_dim_territory


import psycopg2
KAFKA_PATH_CHECKPOINT = '/checkpoint'
db_ops.create_table()

# create sparkSession
conf = Config()
spark_conf = conf.spark_conf
kafka_conf = conf.kafka_conf
kafka_conf.update({
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000",
        "startingOffsets": "earliest",
        "auto.offset.reset": "earliest"
    })

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
log = Log4j(spark)

log.info(f"spark_conf: {spark_conf.getAll()}")
log.info(f"kafka_conf: {kafka_conf.items()}")
# create dim date,product,territory
dim_date = create_dim_date(spark)
dim_product = create_dim_product(spark)
dim_territory = create_dim_territory(spark)

# insert dim_date,product,territory to database
db_ops.insert_to_dim_date(dim_date)
db_ops.insert_to_dim_territory(dim_territory)
db_ops.insert_to_dim_product(dim_product)
print("insert dim_date,product,territory")


def normalize(df):
    # create structure to transform json to dataframe
    schema = StructType([
    StructField("_id", StringType(), True),
    StructField("time_stamp", LongType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("resolution", StringType(), True),
    StructField("user_id_db", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("local_time", TimestampType(), True),
    StructField("show_recommendation", StringType(), True),
    StructField("current_url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("recommendation", StringType(), True),
    StructField("utm_source", StringType(), True),
    StructField("utm_medium", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("option", ArrayType(StructType([
        StructField("option_label", StringType(), True),
        StructField("option_id", StringType(), True),
        StructField("value_label", StringType(), True),
        StructField("value_id", StringType(), True)
    ])), True)
    ])

    df_converted = df.select(from_json(col("value").cast("string"), schema).alias("data"))
    df_final = df_converted.select(
                               "data.time_stamp", 
                               "data.ip", 
                               "data.user_agent",  
                               "data.store_id", 
                               "data.local_time",  
                               "data.current_url", 
                               "data.referrer_url",   
                               "data.product_id"
                               )
    return df_final


def process_batch(batch_df):
    # generate territory_id
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    date_regex = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    ip_regex = r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"
    df_with_flags = batch_df.withColumn(
    "is_valid",
    when(
        (col("product_id").isNotNull()) & 
        (col("local_time").rlike(date_regex)) & 
        (col("ip").rlike(ip_regex)),
        "valid"
    ).otherwise("invalid")
)
    # Phân loại DataFrame
    valid_df = df_with_flags.filter(col("is_valid") == "valid").drop("is_valid")
    invalid_df = df_with_flags.filter(col("is_valid") == "invalid").drop("is_valid")
    # invalid_df = invalid_df\
    #             .select(
    #                     "time_stamp",
    #                     "ip",
    #                     "user_agent",
    #                     "store_id",
    #                     "local_time",
    #                     "current_url",
    #                     "referrer_url",
    #                     "product_id"
    #                     )
    current_domain =  split(col('current_url'),'/')[2]
    domain_size = size(split(current_domain,r"\."))
    country_code = (split(current_domain,r"\.").getItem(domain_size-1))
    dim_territory_id = abs(hash(country_code))
    valid_df = valid_df\
    .withColumn("tmp_territory_id",dim_territory_id)
    behaviour_df = valid_df.join(dim_territory,valid_df["tmp_territory_id"]==dim_territory["territory_id"],'left')
    # territory_id_handle_null
    gen_territory_id = when(col('territory_id').isNull(),-1).otherwise(col('territory_id'))

    # geneate date_id
    gen_date_id = date_format(col("local_time"),'HHddMMyyyy').cast('long')
    #generate browser_id
    parse_browser_udf = udf(lambda ua:parse(ua).browser.family, returnType=StringType())
    gen_browser_id = abs(hash(col('browser')))

    # generate os_id
    parse_os_udf = udf(lambda ua:parse(ua).os.family, returnType=StringType())
    gen_os_id = abs(hash(col('os')))

    # handle null product_id
    handle_null_product_id = when(col('product_id').isNull(),-1).otherwise(col('product_id'))
    # handle null referrer_url
    handle_referrer_url = when(col('referrer_url').isNull(),"Undefine").otherwise(col('referrer_url'))
    # handle null current_url
    handle_current_url = when(col('current_url').isNull(),"Undefine").otherwise(col('current_url'))

    behaviour_df_genkey = behaviour_df\
                        .withColumn('territory_id',gen_territory_id)\
                        .withColumn('date_id',gen_date_id)\
                        .withColumn('browser',parse_browser_udf('user_agent'))\
                        .withColumn('browser_id',gen_browser_id)\
                        .withColumn('os',parse_os_udf('user_agent'))\
                        .withColumn('os_id',gen_os_id)\
                        .withColumn('product_id',handle_null_product_id)\
                        .withColumn('referrer_url',handle_referrer_url)\
                        .withColumn('currrent_url',handle_current_url)
                        
    
    gen_fact_key =  md5(
                        concat(
                        col("date_id"),
                        col("territory_id"),
                        col("product_id"),
                        col("browser_id"),
                        col("os_id"),
                        col("referrer_url"),
                        col("current_url")))                
    df_fact_view = behaviour_df_genkey \
                .groupBy(col("product_id"),
                        col("territory_id"),
                        col("date_id"),
                        col("os_id"),
                        col("browser_id"),
                        col("current_url"),
                        col("referrer_url"),
                        col("store_id"))\
                .agg(
                    count("*").alias("total_view")
                )\
                .withColumn("id",gen_fact_key)
    df_fact_view = df_fact_view.select(["id"] + [col_name for col_name in df_fact_view.columns if col_name != "id"])
    # fact_view.show()

    # dim browser
    df_dim_browser = behaviour_df_genkey\
                    .select("browser_id",
                            col("browser").alias("browser_name"))\
                    .distinct()
    
    # dim os
    df_dim_os = behaviour_df_genkey\
                .select("os_id",
                        col("os").alias("os_name"))\
                .distinct()
    # df_dim_browser.show()
    # df_dim_os.show()

    # load into database
    print("dataframe fact_view:")
    df_fact_view.show()
    print("dataframe invalid_df:")
    invalid_df.show()
    db_ops.update_to_invalid_df(invalid_df)
    db_ops.upsert_to_dim_browser(df_dim_browser)
    db_ops.upsert_to_dim_os(df_dim_os)
    db_ops.upsert_to_fact_vew(df_fact_view)

def streaming_process():
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()


    query = df.transform(lambda df: normalize(df)) \
        .writeStream \
        .outputMode('append') \
        .foreachBatch(lambda batch_df,  batch_id:process_batch(batch_df))\
        .option("truncate", False) \
        .option("checkpointLocation", "/tmp/spark-checkpoints") \
        .trigger(processingTime="20 seconds") \
        .start() \
        
    query.awaitTermination()
    print("process ending")


if __name__ =="__main__":
    print("start process")
    streaming_process()

