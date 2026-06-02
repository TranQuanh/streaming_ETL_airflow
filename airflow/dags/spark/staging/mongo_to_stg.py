import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType,
    DecimalType, IntegerType, ByteType, BooleanType
)


# ============================================================
# CẤU HÌNH KẾT NỐI — đọc từ biến môi trường, fallback local
# ============================================================
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://admin:123@127.0.0.1:27017/glamira?authSource=admin"  # Đổi 'mongodb' -> '127.0.0.1'
)
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "summary")
CLICKHOUSE_HOST     = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT     = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DB       = os.environ.get("CLICKHOUSE_DB", "glamira_stg")
CLICKHOUSE_USER     = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "123")
CLICKHOUSE_TABLE    = os.environ.get("CLICKHOUSE_TABLE", "stg_events")


# ============================================================
# 1. SPARK SESSION
# ============================================================
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Glamira_Mongo_To_Staging_ClickHouse")

        # --- TỐI ƯU SỐ CORE VÀ BỘ NHỚ CHO MÁY 16 CORE / 32G RAM ---
        # "local[*]" bắt Spark sử dụng tối đa tất cả 16 Cores (32 Threads) của CPU
        .master("local[*]") 
        
        # Cấp 22 GB RAM cho toàn bộ Spark Session (Driver & Executor chạy chung trên local)
        .config("spark.driver.memory", "22g")
        
        # Giảm overhead quản lý bộ nhớ, dồn RAM cho việc tính toán dữ liệu lớn
        .config("spark.memory.fraction", "0.7")
        .config("spark.memory.storageFraction", "0.2")

        # --- CHIA NHỎ PHÂN VÙNG ĐỂ KHÔNG BỊ TRÀN RAM (SPILL TO DISK) ---
        # Với 41 triệu dòng, tăng lên 400 để mỗi Core xử lý ~100k dòng mỗi lượt, cực kỳ nhẹ và nhanh
        .config("spark.sql.shuffle.partitions", "400")

        # --- Adaptive Query Execution ---
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "400") # Khớp với shuffle partitions
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "134217728") # Hạ xuống 128MB cho vừa RAM local

        # --- Serialization & Arrow ---
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        # --- MongoDB Read Tuning (Tăng tốc độ kéo data từ Mongo lên gấp đôi) ---
        .config("spark.mongodb.read.batchSize", "20000") # Nâng lên 20k dòng mỗi batch vì máy khỏe
        .config(
            "spark.mongodb.read.partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner"
        )
        .config("spark.mongodb.read.partitioner.options.samplesPerPartition", "20")

        # --- Connectors ---
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
# 2. SCHEMA ĐỌC THÔ TỪ MONGODB
#    Toàn bộ field có type không nhất quán → String hết,
#    xử lý cast trong bước transform để tránh crash.
# ============================================================
RAW_SCHEMA = StructType([
    StructField("_id",                               StringType(), True),
    StructField("time_stamp",                        StringType(), True),  # int/double/string → String trước
    StructField("ip",                                StringType(), True),
    StructField("user_agent",                        StringType(), True),
    StructField("resolution",                        StringType(), True),
    StructField("user_id_db",                        StringType(), True),
    StructField("device_id",                         StringType(), True),
    StructField("store_id",                          StringType(), True),
    StructField("local_time",                        StringType(), True),  # multi-format string
    StructField("current_url",                       StringType(), True),
    StructField("referrer_url",                      StringType(), True),
    StructField("email_address",                     StringType(), True),
    StructField("collection",                        StringType(), True),
    StructField("collect_id",                        StringType(), True),
    StructField("product_id",                        StringType(), True),
    StructField("cat_id",                            StringType(), True),
    StructField("viewing_product_id",                StringType(), True),
    StructField("price",                             StringType(), True),  # string/null → Decimal sau
    StructField("currency",                          StringType(), True),
    StructField("order_id",                          StringType(), True),  # int/double/string
    StructField("is_paypal",                         StringType(), True),  # bool/null
    StructField("utm_source",                        StringType(), True),  # string/null/bool
    StructField("utm_medium",                        StringType(), True),  # bool/null/string
    StructField("recommendation",                    StringType(), True),  # bool/null
    StructField("show_recommendation",               StringType(), True),
    StructField("recommendation_product_id",         StringType(), True),
    StructField("recommendation_product_position",   StringType(), True),  # int/string/null
    StructField("recommendation_clicked_position",   StringType(), True),  # int/null
    StructField("key_search",                        StringType(), True),
    StructField("api_version",                       StringType(), True),
    StructField("option",                            StringType(), True),  # array/object → JSON string
    StructField("cart_products",                     StringType(), True),  # array → JSON string
])


# ============================================================
# 3. TRANSFORM
# ============================================================
def transform(df):
    """
    Toàn bộ bước clean & cast từ raw → stg.
    Thứ tự quan trọng: clean trước, cast sau.
    """

    # ----------------------------------------------------------
    # Bước 1 — event_id: trích từ {"$oid": "..."} hoặc plain string
    # ----------------------------------------------------------
    df = df.withColumn("event_id",
        F.when(
            F.col("_id").contains("$oid"),
            F.get_json_object(F.col("_id"), "$.$oid")
        ).otherwise(F.col("_id"))
    )

    # ----------------------------------------------------------
    # Bước 2 — event_time: time_stamp đang là Unix Seconds (10 chữ số) → Timestamp chuẩn
    # ----------------------------------------------------------
    df = df.withColumn("event_time",
        F.to_timestamp(F.from_unixtime(F.col("time_stamp")))
    )

    # ----------------------------------------------------------
    # Bước 3 — local_time: thử nhiều format, dùng 'H' thay vì 'HH' để nhận diện giờ < 10 (VD: 7:58:41)
    # ----------------------------------------------------------
    df = df.withColumn("local_time",
        F.coalesce(
            F.to_timestamp(F.col("local_time"), "yyyy-MM-dd H:mm:ss"),       # Sửa HH -> H
            F.to_timestamp(F.col("local_time"), "yyyy-MM-dd'T'H:mm:ss"),     # Sửa HH -> H
            F.to_timestamp(F.col("local_time"), "yyyy-MM-dd'T'H:mm:ssXXX"),  # Sửa HH -> H
            F.to_timestamp(F.col("local_time"), "MM/dd/yyyy H:mm:ss"),       # Sửa HH -> H
            F.lit(None).cast(TimestampType())
        )
    )

    # ----------------------------------------------------------
    # Bước 4 — Trim + empty string → null cho tất cả string field
    # ----------------------------------------------------------
    string_cols = [
        "ip", "user_agent", "resolution", "user_id_db", "device_id",
        "store_id", "current_url", "referrer_url", "email_address",
        "collection", "collect_id", "product_id", "cat_id",
        "viewing_product_id", "currency", "order_id", "utm_source",
        "utm_medium", "show_recommendation", "recommendation_product_id",
        "key_search", "api_version",
    ]
    for c in string_cols:
        df = df.withColumn(c,
            F.when(
                F.trim(F.col(c)) == "", F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col(c)))
        )

    # ----------------------------------------------------------
    # Bước 5 — product_id: merge với viewing_product_id
    #   Các collection recommendation_* không có product_id
    #   nhưng có viewing_product_id → coalesce về 1 field
    # ----------------------------------------------------------
    df = df.withColumn("_raw_prod", F.coalesce(F.col("product_id"), F.col("viewing_product_id")))
    df = df.withColumn("product_id",
        F.when(F.trim(F.col("_raw_prod")) == "", F.lit(None).cast(IntegerType()))
         .otherwise(F.col("_raw_prod").cast(IntegerType()))
    ).drop("_raw_prod")

    # ----------------------------------------------------------
    # Bước 6 — utm_source, utm_medium: bool lỗi → null
    # ----------------------------------------------------------
    def cast_to_boolean(col_name):
        return (
            F.when(F.trim(F.col(col_name)).isin(["true", "True", "1"]), F.lit(True))
             .when(F.trim(F.col(col_name)).isin(["false", "False", "0"]), F.lit(False))
             .otherwise(F.lit(None))
             .cast(BooleanType())
        )
    df = df.withColumn("utm_source", cast_to_boolean("utm_source"))
    df = df.withColumn("utm_medium", cast_to_boolean("utm_medium"))

    # ----------------------------------------------------------
    # Bước 7 — order_id: chuẩn hoá double string "12345.0" → "12345"
    # ----------------------------------------------------------
    df = df.withColumn("order_id",
        F.when(F.col("order_id").isNull(), F.lit(None).cast(StringType()))
        .when(
            F.col("order_id").rlike(r"^\d+\.0$"),
            F.regexp_replace(F.col("order_id"), r"\.0$", "")
        )
        .otherwise(F.col("order_id"))
    )

    # ----------------------------------------------------------
    # Bước 8 — price: string → Decimal(18,2)
    # ----------------------------------------------------------
    df = df.withColumn("price",
        F.when(F.col("price").isNull(), F.lit(None).cast(DecimalType(18, 2)))
        .otherwise(F.col("price").cast(DecimalType(18, 2)))
    )

    # ----------------------------------------------------------
    # Bước 9 — is_paypal, recommendation: bool string → 0/1/null (ByteType)
    # ----------------------------------------------------------
    def bool_to_int(col_name):
        return (
            F.when(F.col(col_name).isin(["true", "True", "1"]), F.lit(1))
            .when(F.col(col_name).isin(["false", "False", "0"]), F.lit(0))
            .otherwise(F.lit(None))
            .cast(ByteType())
        )

    df = df.withColumn("is_paypal", cast_to_boolean("is_paypal"))
    df = df.withColumn("recommendation", cast_to_boolean("recommendation"))

    # ----------------------------------------------------------
    # Bước 10 — recommendation positions: string/int/null → Integer
    # ----------------------------------------------------------
    df = df.withColumn(
        "recommendation_product_position",
        F.col("recommendation_product_position").cast(IntegerType())
    ).withColumn(
        "recommendation_clicked_position",
        F.col("recommendation_clicked_position").cast(IntegerType())
    )

    # ----------------------------------------------------------
    # Bước 11 — option: object/array/empty → chuẩn JSON array string
    # ----------------------------------------------------------
    df = df.withColumn("_opt", F.trim(F.col("option"))) \
       .withColumn("_opt", F.regexp_replace(F.col("_opt"), '"category id"', '"category_id"')) \
       .withColumn("option_json",
            F.when(F.col("_opt").isNull(), F.lit(None).cast(StringType()))
            .when(F.col("_opt").isin(["", "[]", "[{}]", "{}"]), F.lit(None).cast(StringType()))
            .when(F.col("_opt").startswith("{"),  #   ĐÃ SỬA: chữ w viết thường
                  F.concat(F.lit("["), F.col("_opt"), F.lit("]")))
            .otherwise(F.col("_opt"))
       ).drop("_opt")

    # ----------------------------------------------------------
    # Bước 12 — cart_products: giữ JSON string, null nếu rỗng
    # ----------------------------------------------------------
    df = df.withColumn("cart_products_json",
        F.when(
            F.trim(F.col("cart_products")).isin(["", "[]", "null"]),
            F.lit(None).cast(StringType())
        ).when(F.col("cart_products").isNull(), F.lit(None).cast(StringType()))
        .otherwise(F.trim(F.col("cart_products")))
    )
    df = df.withColumn("recommendation_product_id", F.col("recommendation_product_id").cast(StringType()))
    df = df.withColumn("recommendation_clicked_position", F.col("recommendation_clicked_position").cast(IntegerType()))
    # ----------------------------------------------------------
    # Bỏ cột gốc không cần ghi xuống ClickHouse
    # ----------------------------------------------------------
    df = df.drop("_id", "option", "cart_products", "time_stamp", "viewing_product_id")

    return df


# ============================================================
# 4. VALIDATE — single pass, không shuffle
# ============================================================
def validate(df):
    """
    Kiểm tra chất lượng data sau transform.
    Dùng single aggregation pass → không trigger shuffle.
    Duplicate sẽ được xử lý bởi ReplacingMergeTree của ClickHouse.
    """
    print("[VALIDATE] Đang kiểm tra chất lượng dữ liệu...")

    stats = df.select(
        F.count(F.when(F.col("event_id").isNull(), 1))
         .alias("null_event_id"),
        F.count(F.when(F.col("event_time").isNull(), 1))
         .alias("null_event_time"),
        F.count(F.when(F.col("collection").isNull(), 1))
         .alias("null_collection"),
        F.count(F.when(
            F.col("event_time") < F.lit("2015-01-01").cast(TimestampType()), 1
        )).alias("suspicious_time"),
        F.count(F.when(
            F.col("price").isNotNull() & (F.col("price") < 0), 1
        )).alias("negative_price"),
        F.count("*").alias("total"),
    ).collect()[0]

    print(f"[VALIDATE] Tổng record: {stats['total']:,}")

    errors = []
    if stats["null_event_id"] > 0:
        errors.append(f"  event_id null: {stats['null_event_id']:,} dòng")
    if stats["null_event_time"] > 0:
        errors.append(f"  event_time null: {stats['null_event_time']:,} dòng — time_stamp bị lỗi")
    if stats["null_collection"] > 0:
        errors.append(f"  collection null: {stats['null_collection']:,} dòng")
    if stats["negative_price"] > 0:
        errors.append(f"  price âm: {stats['negative_price']:,} dòng")

    if errors:
        raise ValueError("❌ Validation thất bại:\n" + "\n".join(errors))

    if stats["suspicious_time"] > 0:
        print(f"[VALIDATE] ⚠️  WARN: {stats['suspicious_time']:,} dòng có event_time trước 2015 — kiểm tra lại data gốc")

    print("[VALIDATE] ✅ Data hợp lệ. Tiến hành ghi vào ClickHouse.")


# ============================================================
# 5. GHI VÀO CLICKHOUSE
# ============================================================
def write_to_clickhouse(df):
    print(f"[WRITE] Ghi vào {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}...")
    
    # 1. Tăng số lượng repartition lên (chia nhỏ mảnh dữ liệu để ghi song song nhẹ hơn)
    # 41 triệu dòng chia cho 32 partitions => mỗi partition chỉ còn tầm 1.3 triệu dòng
    df_partitioned = df.repartition(32)
    
    clickhouse_url = "jdbc:clickhouse://127.0.0.1:8123/glamira_stg?http_connection_provider=HTTP_URL_CONNECTION"
    
    df_partitioned.write \
      .format("jdbc") \
      .option("url",      clickhouse_url) \
      .option("dbtable",  CLICKHOUSE_TABLE) \
      .option("user",     CLICKHOUSE_USER) \
      .option("password", CLICKHOUSE_PASSWORD) \
      .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
      .option("batchsize", "20000") \
      .option("isolationLevel", "NONE") \
      .option("rewriteBatchedStatements", "true") \
      .mode("append") \
      .save()

# ============================================================
# 6. MAIN
# ============================================================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("[READ] Đọc dữ liệu từ MongoDB...")
    df_raw = (
        spark.read
        .format("mongodb")
        .option("spark.mongodb.read.connection.uri", MONGO_URI)
        .option("collection", MONGO_COLLECTION)
        .option("spark.mongodb.read.partitionerOptions.partitionSizeMB", "64")
        .option("spark.mongodb.read.partitionerOptions.samplesPerPartition", "10")
        .schema(RAW_SCHEMA)
        .load()
    )
    print(f"[READ] Schema đọc xong. Bắt đầu transform...")

    df_transformed = transform(df_raw)
    validate(df_transformed)
    write_to_clickhouse(df_transformed)

    spark.stop()
    print("[DONE] Job hoàn tất.")


if __name__ == "__main__":
    main()