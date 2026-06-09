import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ============================================================
# CẤU HÌNH KẾT NỐI
# ============================================================
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:123@127.0.0.1:27017/glamira?authSource=admin")
CLICKHOUSE_URL = "jdbc:clickhouse://127.0.0.1:8123/glamira_stg?http_connection_provider=HTTP_URL_CONNECTION"
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "123")

# ============================================================
# SPARK SESSION (Tối ưu 16 cores / 32GB RAM cho Dữ liệu danh mục)
# ============================================================
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Glamira_Mongo_To_Staging_Final_Dimensions")
        .master("local[*]") 
        .config("spark.driver.memory", "12g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8") 
        .config(
            "spark.jars.packages",
            ",".join([
                "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
                "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0",
                "com.clickhouse:clickhouse-http-client:0.6.3",
            ])
        )
        .getOrCreate()
    )

# ============================================================
# SCHEMAS KHỚP CHUẨN JSON TRONG MONGODB
# ============================================================
COUNTRY_RAW_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("alpha-2", StringType(), True),
    StructField("alpha-3", StringType(), True),
    StructField("country-code", StringType(), True),
    StructField("iso_3166-2", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sub-region", StringType(), True),
    StructField("intermediate-region", StringType(), True),
    StructField("region-code", StringType(), True),
    StructField("sub-region-code", StringType(), True),
    StructField("intermediate-region-code", StringType(), True)
])

PRODUCTS_RAW_SCHEMA = StructType([
    StructField("_id", StringType(), True),          
    StructField("product_name", StringType(), True)  
])

# Schema bóc tách IP từ MongoDB (Giữ nguyên typo của Mongo để đọc đúng dữ liệu)
IP_RAW_SCHEMA = StructType([
    StructField("ip", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("contry_name", StringType(), True),    # Typo từ nguồn phát sinh
    StructField("region_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longtitude", StringType(), True),    # Typo từ nguồn phát sinh
    StructField("ZIP_code", StringType(), True),      # Typo từ nguồn phát sinh
    StructField("time_zone", StringType(), True)
])

def write_to_clickhouse(df, table_name):
    print(f"[WRITE] Ghi dữ liệu vào ClickHouse table: {table_name}...")
    df.write \
      .format("jdbc") \
      .option("url",      CLICKHOUSE_URL) \
      .option("dbtable",  table_name) \
      .option("user",     CLICKHOUSE_USER) \
      .option("password", CLICKHOUSE_PASSWORD) \
      .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
      .option("batchsize", "20000") \
      .option("isolationLevel", "NONE") \
      .option("rewriteBatchedStatements", "true") \
      .mode("append") \
      .save()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # # --------------------------------------------------------
    # # XỬ LÝ BẢNG 1: COUNTRY
    # # --------------------------------------------------------
    # print("\n--- [PROCESS] Xử lý dữ liệu COUNTRY ---")
    # df_country_raw = spark.read.format("mongodb") \
    #     .option("spark.mongodb.read.connection.uri", MONGO_URI) \
    #     .option("collection", "countries") \
    #     .schema(COUNTRY_RAW_SCHEMA).load()

    # df_country_clean = df_country_raw \
    #     .withColumnRenamed("alpha-2", "alpha_2") \
    #     .withColumnRenamed("alpha-3", "alpha_3") \
    #     .withColumnRenamed("country-code", "country_code") \
    #     .withColumnRenamed("iso_3166-2", "iso_3166_2") \
    #     .withColumnRenamed("sub-region", "sub_region") \
    #     .withColumnRenamed("intermediate-region", "intermediate_region") \
    #     .withColumnRenamed("region-code", "region_code") \
    #     .withColumnRenamed("sub-region-code", "sub_region_code") \
    #     .withColumnRenamed("intermediate-region-code", "intermediate_region_code")

    # write_to_clickhouse(df_country_clean, "stg_country")
    # print("✅ Đã đồng bộ xong bảng COUNTRY.")

    # # --------------------------------------------------------
    # # XỬ LÝ BẢNG 2: PRODUCTS
    # # --------------------------------------------------------
    # print("\n--- [PROCESS] Xử lý dữ liệu PRODUCTS ---")
    # df_products_raw = spark.read.format("mongodb") \
    #     .option("spark.mongodb.read.connection.uri", MONGO_URI) \
    #     .option("collection", "products") \
    #     .schema(PRODUCTS_RAW_SCHEMA).load()

    # df_products_clean = df_products_raw \
    #     .withColumn("product_id", F.col("_id").cast(IntegerType())) \
    #     .filter(F.col("product_id").isNotNull()) \
    #     .select(
    #         "product_id",
    #         F.coalesce(F.nullif(F.trim(F.col("product_name")), F.lit("")), F.lit("UNKNOWN")).alias("product_name")
    #     )

    # write_to_clickhouse(df_products_clean, "stg_products")
    # print("✅ Đã đồng bộ xong bảng PRODUCTS.")

    # --------------------------------------------------------
    # XỬ LÝ BẢNG 3: IP (GEOIP) - MỚI THÊM VÀO
    # --------------------------------------------------------
    print("\n--- [PROCESS] Xử lý dữ liệu IP (GEOIP) ---")
    df_ip_raw = spark.read.format("mongodb") \
        .option("spark.mongodb.read.connection.uri", MONGO_URI) \
        .option("collection", "geoip") \
        .schema(IP_RAW_SCHEMA).load()

    # Chuẩn hóa chuỗi và mapping lại các cột bị sai chính tả từ Mongo sang ClickHouse
    df_ip_clean = df_ip_raw.select(
        F.trim(F.col("ip")).alias("ip"),
        F.nullif(F.trim(F.col("country_code")), F.lit("")).alias("country_code"),
        F.nullif(F.trim(F.col("contry_name")), F.lit("")).alias("country_name"),     # Sửa contry -> country
        F.nullif(F.trim(F.col("region_name")), F.lit("")).alias("region_name"),
        F.nullif(F.trim(F.col("city_name")), F.lit("")).alias("city_name"),
        F.nullif(F.trim(F.col("latitude")), F.lit("")).alias("latitude"),
        F.nullif(F.trim(F.col("longtitude")), F.lit("")).alias("longitude"),         # Sửa longtitude -> longitude
        F.nullif(F.trim(F.col("ZIP_code")), F.lit("")).alias("zip_code"),            # Sửa ZIP_code -> zip_code
        F.nullif(F.trim(F.col("time_zone")), F.lit("")).alias("time_zone")
    ).filter(F.col("ip").isNotNull() & (F.col("ip") != ""))

    write_to_clickhouse(df_ip_clean, "stg_ip")
    print("✅ Đã đồng bộ xong bảng IP (GEOIP).")

    spark.stop()
    print("\n[DONE] Toàn bộ quy trình hoàn tất mỹ mãn!")

if __name__ == "__main__":
    main()