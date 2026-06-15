import os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col, split, size, abs as spark_abs, hash as spark_hash,udf,
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

URL_READ_STG  = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_STG}?http_connection_provider=HTTP_URL_CONNECTION"
URL_WRITE_DW  = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DW}?http_connection_provider=HTTP_URL_CONNECTION"


# ============================================================
# SPARK SESSION TỐI ƯU HÓA
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


def write_ch_fact(df, table_name, order_by_cols="date_id, ip_id, product_id"):
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
# CẤU HÌNH ĐƯỜNG DẪN FILE BIN CỦA BẠN
# ============================================================
IP2LOCATION_DB_PATH = r"/home/quanh/streaming_ETL_airflow/airflow/dags/spark/warehouse/data/IP2LOCATION-LITE-DB11.BIN"

# Định nghĩa cấu trúc lưu trữ cho dữ liệu IP trong Spark
IP_INFO_SPARK_SCHEMA = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("region_name",  StringType(), True),
    StructField("city_name",    StringType(), True),
    StructField("latitude",     StringType(), True),
    StructField("longitude",    StringType(), True),
    StructField("zip_code",     StringType(), True),
    StructField("time_zone",    StringType(), True)
])

# ============================================================
# 🔥 HÀM TRÍCH XUẤT IP2LOCATION CHO TỪNG DÒNG (LOGIC CHÍNH CỦA BẠN)
# ============================================================
_ip2location_db = None

def extract_ip_row_by_row(ip_str):
    global _ip2location_db
    
    if not ip_str or ip_str.strip() == "":
        return ("Unknown", "Unknown", "", "", "0.0", "0.0", "", "")
        
    # Chỉ mở file BIN 1 lần duy nhất khi Worker được khởi tạo
    if _ip2location_db is None:
        import IP2Location
        if not os.path.exists(IP2LOCATION_DB_PATH):
            # Nếu sai đường dẫn, dòng này sẽ làm sập Spark ngay lập tức để bạn biết chính xác lỗi ở đâu
            raise FileNotFoundError(f"Không tìm thấy file IP2Location BIN tại đường dẫn: {IP2LOCATION_DB_PATH}")
        _ip2location_db = IP2Location.IP2Location(IP2LOCATION_DB_PATH)
        
    try:
        rec = _ip2location_db.get_all(ip_str.strip())
        return (
            str(rec.country_short) if rec.country_short else "Unknown",
            str(rec.country_long) if rec.country_long else "Unknown",
            str(rec.region) if rec.region else "",
            str(rec.city) if rec.city else "",
            str(rec.latitude) if rec.latitude else "0.0",
            str(rec.longitude) if rec.longitude else "0.0",
            str(rec.zipcode) if rec.zipcode else "",
            str(rec.timezone) if rec.timezone else ""
        )
    except Exception as e:
        # Nếu lỗi trong quá trình bóc tách (Ví dụ: IP nội bộ 127.0.0.1, IP ảo dạng chuỗi lỗi) 
        # thì trả về Unknown để pipeline không bị sập giữa chừng
        return ("Unknown", "Unknown", "", "", "0.0", "0.0", "", str(e))

# Đăng ký hàm với Spark để xử lý phân tán Row-by-Row
extract_ip_udf = udf(extract_ip_row_by_row, IP_INFO_SPARK_SCHEMA)


# ============================================================
# 🔥 HÀM ĐỌC TỪNG IP, TRÍCH XUẤT VÀ ĐẨY THẲNG VÀO DIM_IP
# ============================================================
def sync_dim_ip_from_events(spark, stg_events_df):
    print("\n=== [DIM_IP_SYNC] Bắt đầu đọc từng hàng dữ liệu để trích xuất IP bằng IP2Location ===")
    
    # 1. Đọc nhanh danh sách IP đã có sẵn ở DW về để làm màng lọc (tránh ghi trùng IP đã tồn tại)
    try:
        existing_dim_ip = read_ch(spark, CLICKHOUSE_DW, "dim_ip").select(col("ip_id").alias("dim_ip_id")).distinct()
    except Exception:
        # Nếu bảng dim_ip chưa tồn tại thì tạo DataFrame rỗng
        existing_dim_ip = spark.createDataFrame([], StructType([StructField("dim_ip_id", LongType(), True)]))

    # 2. Lấy danh sách IP từ từng dòng của bảng staging, loại bỏ trùng lặp ngay trong batch này
    raw_unique_ips = (
        stg_events_df
        .filter(col("ip").isNotNull() & (trim(col("ip")) != ""))
        .select(trim(col("ip")).alias("ip"))
        .distinct()
        .withColumn("ip_id", spark_abs(spark_hash(col("ip"))).cast(LongType()))
    )

    # 3. Lọc bỏ các IP đã tồn tại trong Kho dữ liệu (Left Anti Join)
    new_ips_to_process = raw_unique_ips.join(
        existing_dim_ip, 
        raw_unique_ips["ip_id"] == existing_dim_ip["dim_ip_id"], 
        how="left_anti"
    )
    
    new_count = new_ips_to_process.count()
    print(f"[DIM_IP_SYNC] Tìm thấy {new_count:,} IP mới từ các dòng dữ liệu cần giải mã.")

    if new_count > 0:
        # 4. Thực thi trích xuất thông tin chi tiết bằng IP2Location BIN cho từng IP
        dim_ip_records = (
            new_ips_to_process
            .withColumn("_geo", extract_ip_udf(col("ip"))) # Gọi hàm trích xuất của bạn tại đây
            .select(
                col("ip_id"),
                col("ip"),
                col("_geo.country_code").alias("country_code"),
                col("_geo.country_name").alias("country_name"),
                col("_geo.region_name").alias("region_name"),
                col("_geo.city_name").alias("city_name"),
                col("_geo.latitude").alias("latitude"),
                col("_geo.longitude").alias("longitude"),
                col("_geo.zip_code").alias("zip_code"),
                col("_geo.time_zone").alias("time_zone")
            )
        )
        
        # 5. Đẩy THẲNG tập dữ liệu đã trích xuất vào bảng dim_ip trên ClickHouse DW
        print(f"[WRITE] Đang lưu thẳng {new_count:,} dữ liệu IP2Location vào glamira_dw.dim_ip...")
        dim_ip_records.write \
            .format("jdbc") \
            .option("url",      URL_WRITE_DW) \
            .option("dbtable",  "dim_ip") \
            .option("user",     CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASSWORD) \
            .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "50000") \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
        print("[DIM_IP_SYNC] Hoàn tất trích xuất và nạp thẳng dim_ip!")
    else:
        print("[DIM_IP_SYNC] Không có IP mới phát sinh trong dòng dữ liệu sự kiện.")


# ============================================================
# 1. CORE PIPELINE: BUILD FACT ORDER
# ============================================================
def build_fact_order(spark, stg_events_df):
    print("\n=== [FACT_ORDER] Bắt đầu tính toán và xử lý ETL ===")

    # 1. Sử dụng DataFrame truyền vào và lọc các sự kiện checkout thành công
    stg_events = stg_events_df.filter(col("collection") == "checkout_success")

    # 2. Đọc bảng tỷ giá từ Data Warehouse để xử lý tính toán doanh thu USD
    dim_currency = read_ch(spark, CLICKHOUSE_DW, "dim_currency").select("currency_id", "usd_conversion_rate")

    # 3. Phân rã mảng giỏ hàng (Explode sang từng sản phẩm độc lập)
    exploded_df = (
        stg_events
        .filter(col("cart_products_json").isNotNull())
        .withColumn("_carts", from_json(col("cart_products_json"), CART_PRODUCT_SCHEMA))
        .withColumn("_cart", explode(col("_carts")))
    )

    # 4. Kỹ thuật Array Higher-Order Functions để lấy Option không làm tăng số dòng
    processed_df = (
        exploded_df
        .withColumn("_alloy_label", expr("filter(_cart.option, x -> lower(trim(x.option_label)) == 'alloy')[0].value_label"))
        .withColumn("_diamond_label", expr("filter(_cart.option, x -> lower(trim(x.option_label)) == 'diamond')[0].value_label"))
    )

    # 5. Khối chuẩn hóa dữ liệu, băm ID kết nối Dimension và dọn dẹp Price gãy
    fact_order = (
        processed_df
        .withColumn("event_id", coalesce(col("event_id"), lit("Unknown")))
        .withColumn("order_id", coalesce(col("order_id"), lit("Unknown")))
        .withColumn("user_id_db", col("user_id_db").cast(LongType()))

        # --- Khóa thời gian thô ---
        .withColumn("_chosen_time", coalesce(col("event_time"), col("event_time")))
        .withColumn("date_id_raw", 
            when(col("_chosen_time").isNull(), lit(-1).cast(LongType()))
            .otherwise(date_format(col("_chosen_time"), "HHddMMyyyy").cast(LongType()))
        )

        # --- Sinh khóa IP thô ---
        .withColumn("ip_id_raw",
            when(col("ip").isNull() | (trim(col("ip")) == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(trim(col("ip")))).cast(LongType()))
        )

        # --- Khóa quốc gia lãnh thổ thô ---
        .withColumn("_domain", split(col("current_url"), "/").getItem(2))
        .withColumn("country_code", split(col("_domain"), r"\.").getItem(size(split(col("_domain"), r"\."))-1))
        .withColumn("territory_id_raw",
            when(col("country_code").isNull() | (trim(col("country_code")) == "") | (trim(col("country_code")) == "com"), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("country_code"))).cast(LongType()))
        )

        # --- Các khóa thuộc tính nguyên liệu ---
        .withColumn("alloy_id",
            when(col("_alloy_label").isNull() | (trim(col("_alloy_label")) == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("_alloy_label"))).cast(LongType()))
        )
        .withColumn("diamond_id",
            when(col("_diamond_label").isNull() | (trim(col("_diamond_label")) == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("_diamond_label"))).cast(LongType()))
        )

        # --- Các khóa chiều khác ---
        .withColumn("device_id_raw", coalesce(trim(col("device_id")), lit("Unknown")))
        .drop("device_id") 
        .withColumn("product_id", coalesce(col("_cart.product_id").cast(LongType()), lit(-1).cast(LongType())))
        
        # --- Đồng bộ khóa đơn vị tiền tệ sang dim_currency ---
        .withColumn("_curr_clean", trim(regexp_replace(col("_cart.currency"), r"[\u00a0\s]+", " ")))
        .withColumn("currency_id",
            when(col("_curr_clean").isNull() | (col("_curr_clean") == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("_curr_clean"))).cast(LongType()))
        )

        # --- Chuyển đổi trạng thái Flag show_recommendation sang định dạng Số ---
        .withColumn("show_recommendation",
            when(lower(trim(col("show_recommendation"))) == "true", lit(1).cast(IntegerType()))
            .otherwise(lit(0).cast(IntegerType()))
        )

        # --- Chỉ số đo lường ---
        .withColumn("amount", coalesce(col("_cart.amount").cast(IntegerType()), lit(1)))
        .withColumn("_price_clean_1", regexp_replace(col("_cart.price"), r"[\s\u00A0']", ""))
        .withColumn("_price_clean_2", regexp_replace(col("_price_clean_1"), r",(\d{2})$", r".\1"))
        .withColumn("_price_clean_3", regexp_replace(col("_price_clean_2"), r"[.,](?=\d{3})", ""))
        .withColumn("_price_clean_4", regexp_replace(col("_price_clean_3"), r",", "."))
        .withColumn("price_local", coalesce(col("_price_clean_4").cast(DoubleType()), lit(0.0)))
    )

    # --- 6. DATA QUALITY CHECK: Đối chiếu kiểm tra tính toàn vẹn khóa ngoại ---
    dim_date = read_ch(spark, CLICKHOUSE_DW, "dim_date").select(col("date_id").alias("dim_date_id")).distinct()
    dim_territory = read_ch(spark, CLICKHOUSE_DW, "dim_territory").select(col("territory_id").alias("dim_territory_id")).distinct()
    dim_device = read_ch(spark, CLICKHOUSE_DW, "dim_device").select(col("device_id").alias("dim_device_id")).distinct()
    
    # Đọc bảng dim_ip đã được cập nhật mới nhất ở bước Sync trước đó
    dim_ip = read_ch(spark, CLICKHOUSE_DW, "dim_ip").select(col("ip_id").alias("dim_ip_id")).distinct()

    fact_order_validated = (
        fact_order
        .join(dim_date, col("date_id_raw") == col("dim_date_id"), how="left")
        .withColumn("date_id", when(col("dim_date_id").isNotNull(), col("date_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_territory, col("territory_id_raw") == col("dim_territory_id"), how="left")
        .withColumn("territory_id", when(col("dim_territory_id").isNotNull(), col("territory_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_device, col("device_id_raw") == col("dim_device_id"), how="left")
        .withColumn("device_id", when(col("dim_device_id").isNotNull(), col("device_id_raw")).otherwise(lit("-1").cast(StringType())))
        
        # 🔎 KIỂM TRA IP: Nếu có bên Dim thì lấy, không có (hoặc lỗi lọc) phạt về -1
        .join(dim_ip, col("ip_id_raw") == col("dim_ip_id"), how="left")
        .withColumn("ip_id", when(col("dim_ip_id").isNotNull(), col("ip_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_currency, on="currency_id", how="left")
        .withColumn("price_usd", col("price_local") * col("usd_conversion_rate"))
    )

    fact_order_final = fact_order_validated.select(
        "event_id", "order_id", "date_id", "ip_id", "territory_id", "device_id",
        "product_id", "currency_id", "alloy_id", "diamond_id", "user_id_db",
        "show_recommendation", "amount", "price_local", "price_usd"
    )

    print(f"[FACT_ORDER] Xử lý thành công. Tổng số bản ghi: {fact_order_final.count():,}")
    return fact_order_final


# ============================================================
# 2. CORE PIPELINE: BUILD FACT EVENTS
# ============================================================
def build_fact_events(spark, stg_events_df):
    print("\n=== [FACT_EVENTS] Bắt đầu tính toán và xử lý ETL ===")

    # 1. Loại bỏ các sự kiện liên quan đến Đơn hàng từ DataFrame truyền vào
    stg_events = stg_events_df.filter(~col("collection").isin("checkout", "checkout_success"))

    # 2. Xử lý chuẩn hóa và định hình Khóa ngoại thô trước khi đối chiếu
    parsed_events = (
        stg_events
        .withColumn("event_id", coalesce(col("event_id"), lit("Unknown")).cast(StringType()))
        .withColumn("user_id_db", when(trim(col("user_id_db")) == "", lit(None)).otherwise(col("user_id_db")).cast(LongType()))
        
        # --- Khóa thời gian thô ---
        .withColumn("_time_col", coalesce(col("event_time"), col("event_time")))
        .withColumn("date_id_raw", 
            when(col("_time_col").isNull(), lit(-1).cast(LongType()))
            .otherwise(date_format(col("_time_col"), "HHddMMyyyy").cast(LongType()))
        )
        
        # --- Sinh khóa IP thô ---
        .withColumn("ip_id_raw",
            when(col("ip").isNull() | (trim(col("ip")) == ""), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(trim(col("ip")))).cast(LongType()))
        )
        
        # --- Khóa quốc gia lãnh thổ thô ---
        .withColumn("_domain", split(col("current_url"), "/").getItem(2))
        .withColumn("country_code", split(col("_domain"), r"\.").getItem(size(split(col("_domain"), r"\."))-1))
        .withColumn("territory_id_raw",
            when(col("country_code").isNull() | (trim(col("country_code")) == "") | (trim(col("country_code")) == "com"), lit(-1).cast(LongType()))
            .otherwise(spark_abs(spark_hash(col("country_code"))).cast(LongType()))
        )
        
        # --- Các khóa chiều khác ---
        .withColumn("device_id_raw", coalesce(trim(col("device_id")), lit("Unknown")))
        .withColumn("product_id", coalesce(col("product_id").cast(LongType()), lit(-1).cast(LongType())))
        .drop("device_id") 
        
        .withColumn("store_id", trim(col("store_id")))
        .withColumn("utm_source", when(col("utm_source") == True, lit("Unknown-Paid-Source")).otherwise(lit("Organic")))
        .withColumn("utm_medium", when(col("utm_medium") == True, lit("Unknown-Paid-Medium")).otherwise(lit("Organic")))
        .withColumn("current_url", when(col("current_url").isNull(), lit("Unknown")).otherwise(trim(col("current_url"))))
        
        .withColumn("event_count", lit(1).cast(IntegerType()))
    )

    # --- 3. DATA QUALITY CHECK ---
    dim_date = read_ch(spark, CLICKHOUSE_DW, "dim_date").select(col("date_id").alias("dim_date_id")).distinct()
    dim_territory = read_ch(spark, CLICKHOUSE_DW, "dim_territory").select(col("territory_id").alias("dim_territory_id")).distinct()
    dim_device = read_ch(spark, CLICKHOUSE_DW, "dim_device").select(col("device_id").alias("dim_device_id")).distinct()
    dim_ip = read_ch(spark, CLICKHOUSE_DW, "dim_ip").select(col("ip_id").alias("dim_ip_id")).distinct()

    print("[QUALITY CHECK] Đang rà soát và xác thực tính toàn vẹn liên kết thực thể Fact Events...")
    fact_events_validated = (
        parsed_events
        .join(dim_date, col("date_id_raw") == col("dim_date_id"), how="left")
        .withColumn("date_id", when(col("dim_date_id").isNotNull(), col("date_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_territory, col("territory_id_raw") == col("dim_territory_id"), how="left")
        .withColumn("territory_id", when(col("dim_territory_id").isNotNull(), col("territory_id_raw")).otherwise(lit(-1).cast(LongType())))
        
        .join(dim_device, col("device_id_raw") == col("dim_device_id"), how="left")
        .withColumn("device_id", when(col("dim_device_id").isNotNull(), col("device_id_raw")).otherwise(lit("-1").cast(StringType())))
        
        # 🔎 KIỂM TRA IP
        .join(dim_ip, col("ip_id_raw") == col("dim_ip_id"), how="left")
        .withColumn("ip_id", when(col("dim_ip_id").isNotNull(), col("ip_id_raw")).otherwise(lit(-1).cast(LongType())))
    )

    fact_events_final = fact_events_validated.select(
        "event_id", "collection", "date_id", "ip_id", "territory_id", "device_id",
        "store_id", "user_id_db", "product_id", "current_url", "utm_source", "utm_medium", "event_count"
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
        # Bước Tối Ưu: Chỉ đọc stg_events ĐÚNG 1 LẦN duy nhất từ ClickHouse Staging
        stg_events_df = read_ch(spark, CLICKHOUSE_STG, "stg_events")
        
        # [1] Đồng bộ động tầng Dimension IP (Tự trích xuất IP mới từ dữ liệu thô và nạp vào DW)
        # sync_dim_ip_from_events(spark, stg_events_df)
        
        # [2] Thực thi xử lý và ghi dữ liệu bảng Fact Order
        fact_order_df = build_fact_order(spark, stg_events_df)
        write_ch_fact(fact_order_df, "fact_order", order_by_cols="date_id, ip_id, product_id")
        
        # [3] Thực thi xử lý và ghi dữ liệu bảng Fact Events (Gỡ comment để chạy đồng thời)
        fact_events_df = build_fact_events(spark, stg_events_df)
        write_ch_fact(fact_events_df, "fact_events", order_by_cols="date_id, ip_id, product_id")

        print("\n[THÀNH CÔNG RỰC RỠ] Quy trình nạp Fact và đồng bộ tự động Dim IP hoàn tất.")
        
    except Exception as e:
        print(f"\n[LỖI HỆ THỐNG] Quy trình Pipeline tầng Fact thất bại: {str(e)}")
        raise e
    finally:
        spark.stop()
        print("=== ĐÃ ĐÓNG SPARK SESSION AN TOÀN ===")


if __name__ == "__main__":
    main()