import os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col, split, size, abs as spark_abs, hash as spark_hash,
    date_format, trim, when, lit, explode, from_json, coalesce, lower, expr, regexp_replace
)
from pyspark.sql.types import ArrayType, StructType, StructField

# ============================================================
# CẤU HÌNH HỆ THỐNG
# ============================================================
CLICKHOUSE_HOST     = os.environ.get("CLICKHOUSE_HOST",     "127.0.0.1")
CLICKHOUSE_PORT     = os.environ.get("CLICKHOUSE_PORT",     "8123")
CLICKHOUSE_DW       = os.environ.get("CLICKHOUSE_DW",       "glamira_dw")
CLICKHOUSE_STG      = os.environ.get("CLICKHOUSE_STG",      "glamira_stg")
CLICKHOUSE_USER     = os.environ.get("CLICKHOUSE_USER",     "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "123")

# URL Tách biệt rõ ràng để đọc Staging và đọc/ghi vào Data Warehouse
URL_READ_STG  = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_STG}?http_connection_provider=HTTP_URL_CONNECTION"
URL_WRITE_DW  = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DW}?http_connection_provider=HTTP_URL_CONNECTION"


# ============================================================
# SPARK SESSION TỐI ƯU HÓA (Đồng bộ cấu hình hệ thống)
# ============================================================
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Glamira_Build_Fact_Pipeline")
        .master("local[*]")
        .config("spark.driver.memory", "10g")
        .config("spark.memory.fraction", "0.7")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.jars.packages",
            ",".join([
                "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0",
                "com.clickhouse:clickhouse-http-client:0.6.3",
            ])
        )
        .getOrCreate()
    )


# ============================================================
# HELPER READ/WRITE CLICKHOUSE TỐI ƯU
# ============================================================
def read_ch(spark, db_name, table_name):
    """
    Hàm đọc linh hoạt dữ liệu từ cả Staging Database và Data Warehouse
    """
    url = URL_READ_STG if db_name == CLICKHOUSE_STG else URL_WRITE_DW
    print(f"[READ] Đang đọc dữ liệu từ ClickHouse [{db_name}].[{table_name}]...")
    return spark.read \
        .format("jdbc") \
        .option("url",      url) \
        .option("dbtable",  table_name) \
        .option("user",     CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
        .load()


def write_ch_fact(df, table_name, order_by_cols="date_id, territory_id, product_id"):
    """
    Hàm ghi tối ưu dữ liệu Fact với BatchSize lớn và Khóa sắp xếp (Order By) chuẩn Fact
    """
    print(f"[WRITE] Đang chuẩn bị xử lý và ghi vào ClickHouse DW: {table_name}...")
    
    df.write \
      .format("jdbc") \
      .option("url",      URL_WRITE_DW) \
      .option("dbtable",  table_name) \
      .option("user",     CLICKHOUSE_USER) \
      .option("password", CLICKHOUSE_PASSWORD) \
      .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
      .option("batchsize", "50000") \
      .option("isolationLevel", "NONE") \
      .option("rewriteBatchedStatements", "true") \
      .option("createTableOptions", f"ENGINE = MergeTree() ORDER BY ({order_by_cols})") \
      .mode("append") \
      .save()


# ============================================================
# SCHEMA ĐỂ PARSE JSON (Dùng cho Fact Order)
# ============================================================
CART_OPTION_SCHEMA = ArrayType(StructType([
    StructField("option_label", StringType(), True),
    StructField("option_id",    StringType(), True),
    StructField("value_label",  StringType(), True),
    StructField("value_id",     StringType(), True),
]))

CART_PRODUCT_SCHEMA = ArrayType(StructType([
    StructField("product_id", StringType(), True),
    StructField("amount",     StringType(), True),
    StructField("price",      StringType(), True),
    StructField("currency",   StringType(), True),
    StructField("option",     CART_OPTION_SCHEMA, True),
]))


# ============================================================
# 1. CORE PIPELINE: BUILD FACT ORDER
# ============================================================
def build_fact_order(spark):
    print("\n=== [FACT_ORDER] Bắt đầu tính toán và xử lý ETL ===")

    # 1. Đọc dữ liệu thô từ Staging (Chỉ lọc các sự kiện checkout thành công)
    stg_events = read_ch(spark, CLICKHOUSE_STG, "stg_events").filter(col("collection") == "checkout_success")

    # 2. Đọc bảng tỷ giá từ Data Warehouse để xử lý tính toán doanh thu USD
    dim_currency = read_ch(spark, CLICKHOUSE_DW, "dim_currency").select("currency_id", "usd_conversion_rate")

    # 3. Phân rã mảng giỏ hàng (Explode sang từng sản phẩm độc lập)
    exploded_df = (
        stg_events
        .filter(col("cart_products_json").isNotNull())
        .withColumn("_carts", from_json(col("cart_products_json"), CART_PRODUCT_SCHEMA))
        .withColumn("_cart", explode(col("_carts")))
    )

    # 4. Kỹ thuật Array Higher-Order Functions để lấy Option không làm tăng số dòng (Giữ hạt 1 dòng / 1 sản phẩm)
    processed_df = (
        exploded_df
        .withColumn("_alloy_label", expr("filter(_cart.option, x -> lower(trim(x.option_label)) == 'alloy')[0].value_label"))
        .withColumn("_diamond_label", expr("filter(_cart.option, x -> lower(trim(x.option_label)) == 'diamond')[0].value_label"))
    )

    # 5. Khối chuẩn hóa dữ liệu, băm ID kết nối Dimension và dọn dẹp Price gãy
    fact_order = (
        processed_df
        # --- A. Các trường thông tin định danh (Xử lý Null/Empty) ---
        .withColumn("event_id", coalesce(col("event_id"), lit("Unknown")))
        .withColumn("order_id", coalesce(col("order_id"), lit("Unknown")))
        .withColumn("user_id_db", col("user_id_db").cast(LongType()))

        # --- B. Khóa thời gian thô (date_id_raw) ---
        .withColumn("_chosen_time", coalesce(col("local_time"), col("event_time")))
        .withColumn("date_id_raw", 
            when(col("_chosen_time").isNull(), lit(-1).cast(LongType()))
            .otherwise(date_format(col("_chosen_time"), "HHddMMyyyy").cast(LongType()))
        )

        # --- C. Khóa quốc gia lãnh thổ thô (territory_id_raw) ---
        .withColumn("_domain", split(col("current_url"), "/").getItem(2))
        .withColumn("country_code", split(col("_domain"), r"\.").getItem(size(split(col("_domain"), r"\."))-1))
        .withColumn("territory_id_raw",
            when(col("country_code").isNull() | (trim(col("country_code")) == "") | (trim(col("country_code")) == "com"), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("country_code"))).cast(LongType()))
        )

        # --- D. Các khóa thuộc tính nguyên liệu (Alloy ID & Diamond ID) ---
        .withColumn("alloy_id",
            when(col("_alloy_label").isNull() | (trim(col("_alloy_label")) == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("_alloy_label"))).cast(LongType()))
        )
        .withColumn("diamond_id",
            when(col("_diamond_label").isNull() | (trim(col("_diamond_label")) == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("_diamond_label"))).cast(LongType()))
        )

        # --- E. Các khóa chiều khác ---
        .withColumn("device_id_raw", coalesce(trim(col("device_id")), lit("Unknown")))
        .withColumn("product_id", coalesce(col("_cart.product_id").cast(LongType()), lit(-1).cast(LongType())))
        
        # --- F. Đồng bộ khóa đơn vị tiền tệ sang dim_currency ---
        .withColumn("_curr_clean", trim(col("_cart.currency")))
        .withColumn("currency_id",
            when(col("_curr_clean").isNull() | (col("_curr_clean") == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("_curr_clean"))).cast(LongType()))
        )

        # --- G. Chuyển đổi trạng thái Flag show_recommendation sang định dạng Số ---
        .withColumn("show_recommendation",
            when(lower(trim(col("show_recommendation"))) == "true", lit(1).cast(IntegerType()))
            .otherwise(lit(0).cast(IntegerType()))
        )

        # --- H. Đo lường chuyên sâu (Metrics): Xử lý làm sạch chuỗi giá (4-Step Regex) ---
        .withColumn("amount", coalesce(col("_cart.amount").cast(IntegerType()), lit(1)))
        .withColumn("_price_clean_1", regexp_replace(col("_cart.price"), r"[\s\u00A0']", ""))
        .withColumn("_price_clean_2", regexp_replace(col("_price_clean_1"), r",(\d{2})$", r".\1"))
        .withColumn("_price_clean_3", regexp_replace(col("_price_clean_2"), r"[.,](?=\d{3})", ""))
        .withColumn("_price_clean_4", regexp_replace(col("_price_clean_3"), r",", "."))
        .withColumn("price_local", coalesce(col("_price_clean_4").cast(DoubleType()), lit(0.0)))
    )

    # --- 6. DATA QUALITY CHECK: Đối chiếu kiểm tra tính toàn vẹn khóa ngoại sang hệ Dim ---
    dim_date = read_ch(spark, CLICKHOUSE_DW, "dim_date").select("date_id").distinct()
    dim_territory = read_ch(spark, CLICKHOUSE_DW, "dim_territory").select("territory_id").distinct()
    dim_device = read_ch(spark, CLICKHOUSE_DW, "dim_device").select("device_id").distinct()

    fact_order_validated = (
        fact_order
        .join(dim_date, fact_order["date_id_raw"] == dim_date["date_id"], how="left")
        .withColumn("date_id", when(dim_date["date_id"].isNotNull(), col("date_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_territory, fact_order["territory_id_raw"] == dim_territory["territory_id"], how="left")
        .withColumn("territory_id", when(dim_territory["territory_id"].isNotNull(), col("territory_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_device, fact_order["device_id_raw"] == dim_device["device_id"], how="left")
        .withColumn("device_id", when(dim_device["device_id"].isNotNull(), col("device_id_raw")).otherwise(lit("-1").cast(StringType())))
        
        .join(dim_currency, on="currency_id", how="left")
        .withColumn("price_usd", col("price_local") * col("usd_conversion_rate"))
    )

    fact_order_final = fact_order_validated.select(
        "event_id", "order_id", "date_id", "territory_id", "device_id",
        "product_id", "currency_id", "alloy_id", "diamond_id", "user_id_db",
        "show_recommendation", "amount", "price_local", "price_usd"
    )

    print(f"[FACT_ORDER] Xử lý thành công. Tổng số bản ghi: {fact_order_final.count():,}")
    return fact_order_final


# ============================================================
# 2. CORE PIPELINE: BUILD FACT EVENTS (NEW)
# ============================================================
def build_fact_events(spark):
    print("\n=== [FACT_EVENTS] Bắt đầu tính toán và xử lý ETL ===")

    # 1. Đọc dữ liệu thô và loại bỏ các sự kiện liên quan đến Đơn hàng
    stg_events = read_ch(spark, CLICKHOUSE_STG, "stg_events").filter(
        ~col("collection").isin("checkout", "checkout_success")
    )

    # 2. Xử lý chuẩn hóa và định hình Khóa ngoại thô trước khi đối chiếu
    parsed_events = (
        stg_events
        # --- A. Các trường thông tin định danh ---
        .withColumn("event_id", coalesce(col("event_id"), lit("Unknown")).cast(StringType()))
        .withColumn("user_id_db", when(trim(col("user_id_db")) == "", lit(None)).otherwise(col("user_id_db")).cast(LongType()))
        
        # --- B. Khóa thời gian thô (date_id_raw) ---
        .withColumn("_time_col", coalesce(col("local_time"), col("event_time")))
        .withColumn("date_id_raw", 
            when(col("_time_col").isNull(), lit(-1).cast(LongType()))
            .otherwise(date_format(col("_time_col"), "HHddMMyyyy").cast(LongType()))
        )
        
        # --- C. Khóa quốc gia lãnh thổ thô (territory_id_raw) ---
        .withColumn("_domain", split(col("current_url"), "/").getItem(2))
        .withColumn("country_code", split(col("_domain"), r"\.").getItem(size(split(col("_domain"), r"\."))-1))
        .withColumn("territory_id_raw",
            when(col("country_code").isNull() | (trim(col("country_code")) == "") | (trim(col("country_code")) == "com"), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("country_code"))).cast(LongType()))
        )
        
        # --- E. Các khóa chiều khác ---
        .withColumn("device_id_raw", coalesce(trim(col("device_id")), lit("Unknown")))
        .withColumn("product_id", coalesce(col("product_id").cast(LongType()), lit(-1).cast(LongType())))
        
        # 🔥 ĐÂY LÀ CHÌA KHÓA: Xóa cột device_id gốc của staging đi vì đã có device_id_raw thay thế
        .drop("device_id") 
        
        # --- Chiều đo lường và Dọn dẹp traffic Marketing ---
        .withColumn("store_id", trim(col("store_id")))
        
        # --- CHUẨN HÓA UTM_SOURCE ---
        .withColumn("utm_source", 
            when(col("utm_source") == True, lit("Unknown-Paid-Source"))
            .otherwise(lit("Organic"))
        )
        
        # --- CHUẨN HÓA UTM_MEDIUM ---
        .withColumn("utm_medium", 
            when(col("utm_medium") == True, lit("Unknown-Paid-Medium"))
            .otherwise(lit("Organic"))
        )
        
        .withColumn("current_url", when(col("current_url").isNull(), lit("Unknown")).otherwise(trim(col("current_url"))))
        
        # --- Chỉ số đếm sự kiện ---
        .withColumn("event_count", lit(1).cast(IntegerType()))
    )

    # --- 3. DATA QUALITY CHECK: Tải các bảng Dimension từ DW ---
    dim_date = read_ch(spark, CLICKHOUSE_DW, "dim_date").select("date_id").distinct()
    dim_territory = read_ch(spark, CLICKHOUSE_DW, "dim_territory").select("territory_id").distinct()
    dim_device = read_ch(spark, CLICKHOUSE_DW, "dim_device").select("device_id").distinct()

    # --- 4. Thực thi LEFT JOIN và Phạt chất lượng dữ liệu lỗi ---
    print("[QUALITY CHECK] Đang rà soát và xác thực tính toàn vẹn liên kết thực thể Fact Events...")
    fact_events_validated = (
        parsed_events
        .join(dim_date, parsed_events["date_id_raw"] == dim_date["date_id"], how="left")
        .withColumn("date_id", when(dim_date["date_id"].isNotNull(), col("date_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_territory, parsed_events["territory_id_raw"] == dim_territory["territory_id"], how="left")
        .withColumn("territory_id", when(dim_territory["territory_id"].isNotNull(), col("territory_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        # Lúc này DataFrame chỉ có duy nhất 1 cột device_id (đến từ bảng dim_device) nên không lo bị trùng!
        .join(dim_device, parsed_events["device_id_raw"] == dim_device["device_id"], how="left")
        .withColumn("device_id", 
            when(dim_device["device_id"].isNotNull(), col("device_id_raw"))
            .otherwise(lit("-1").cast(StringType()))
        )
    )

    # 5. Sắp xếp cấu trúc đầu ra cuối cùng sạch sẽ (Chắc chắn hết lỗi Ambiguous)
    fact_events_final = fact_events_validated.select(
        "event_id",
        "collection",
        "date_id",
        "territory_id",
        "device_id",
        "store_id",
        "user_id_db",
        "product_id",
        "current_url",
        "utm_source",
        "utm_medium",
        "event_count"
    )

    print(f"[FACT_EVENTS] Xử lý thành công. Tổng số bản ghi hành vi: {fact_events_final.count():,}")
    return fact_events_final


# ============================================================
# MAIN EXECUTIVE PIPELINE
# ============================================================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Thực thi quy trình xử lý và ghi dữ liệu bảng Fact Order
        # fact_order_df = build_fact_order(spark)
        # write_ch_fact(fact_order_df, "fact_order", order_by_cols="date_id, territory_id, product_id")
        
        # 2. Thực thi quy trình xử lý và ghi dữ liệu bảng Fact Events mới (Đã gỡ bỏ hoàn toàn bộ trường recommendation)
        fact_events_df = build_fact_events(spark)
        write_ch_fact(fact_events_df, "fact_events", order_by_cols="date_id, collection, store_id")

        print("\n[THÀNH CÔNG RỰC RỠ] Đã hoàn thành nạp toàn bộ hệ thống bảng Fact vào DW.")
        
    except Exception as e:
        print(f"\n[LỖI HỆ THỐNG] Tiến trình xử lý Pipeline tầng Fact thất bại: {str(e)}")
        raise e
    finally:
        spark.stop()
        print("=== ĐÃ ĐÓNG SPARK SESSION AN TOÀN ===")


if __name__ == "__main__":
    main()