import os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, IntegerType
from pyspark.sql.functions import (
    col, split, size, abs as spark_abs, hash as spark_hash,
    date_format, dayofmonth, month, year, hour,
    udf, trim, when, lit
)
from ua_parser import user_agent_parser


# ============================================================
# CẤU HÌNH
# ============================================================
CLICKHOUSE_HOST     = os.environ.get("CLICKHOUSE_HOST",     "127.0.0.1")
CLICKHOUSE_PORT     = os.environ.get("CLICKHOUSE_PORT",     "8123")
CLICKHOUSE_DW       = os.environ.get("CLICKHOUSE_DW",       "glamira_dw")
CLICKHOUSE_STG      = os.environ.get("CLICKHOUSE_STG",      "glamira_stg")
CLICKHOUSE_USER     = os.environ.get("CLICKHOUSE_USER",     "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "123")


# ============================================================
# SPARK SESSION
# ============================================================
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Glamira_Build_Dim")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
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
# HELPER
# ============================================================
def read_ch(spark, table: str, db: str = None):
    return (
        spark.read
        .format("clickhouse")
        .option("clickhouse.host",      CLICKHOUSE_HOST)
        .option("clickhouse.http_port", CLICKHOUSE_PORT)
        .option("clickhouse.user",      CLICKHOUSE_USER)
        .option("clickhouse.password",  CLICKHOUSE_PASSWORD)
        .option("clickhouse.database",  db or CLICKHOUSE_STG)
        .load(table)
    )


def write_ch(df, table: str, mode: str = "overwrite"):
    print(f"[WRITE] {table} — {df.count():,} dòng...")
    (
        df.write
        .format("clickhouse")
        .option("clickhouse.host",            CLICKHOUSE_HOST)
        .option("clickhouse.http_port",       CLICKHOUSE_PORT)
        .option("clickhouse.user",            CLICKHOUSE_USER)
        .option("clickhouse.password",        CLICKHOUSE_PASSWORD)
        .option("clickhouse.database",        CLICKHOUSE_DW)
        .option("clickhouse.write.batchSize", "200000")
        .mode(mode)
        .save(table)
    )
    print(f"[WRITE] ✅ {table} xong.")


# ============================================================
# 1. DIM_DATE
# ============================================================
def build_dim_date(spark):
    print("[DIM_DATE] Sinh sequence timestamp 2020...")
    df = spark.sql("""
        SELECT sequence(
            to_timestamp('2020-01-01 00:00:00'),
            to_timestamp('2020-12-31 23:00:00'),
            interval 1 hour
        ) AS full_date
    """).selectExpr("explode(full_date) as full_date")

    df = (
        df
        .withColumn("date_id",
            date_format(col("full_date"), "HHddMMyyyy").cast(LongType()))
        .withColumn("day_of_week",       date_format(col("full_date"), "EEEE"))
        .withColumn("day_of_week_short", date_format(col("full_date"), "E"))
        .withColumn("day_of_month",      dayofmonth(col("full_date")))
        .withColumn("month",             month(col("full_date")))
        .withColumn("year",              year(col("full_date")))
        .withColumn("hour",              hour(col("full_date")))
        .select("date_id", "full_date", "day_of_week", "day_of_week_short",
                "day_of_month", "year", "month", "hour")
    )
    print(f"[DIM_DATE] {df.count():,} rows.")
    return df


# ============================================================
# 2. DIM_TERRITORY
#
# Logic (giữ nguyên logic gốc của bạn):
#   domain       = split(current_url, '/')[2]       → 'www.glamira.de'
#   domain_size  = size(split(domain, '.'))
#   country_code = split(domain, '.')[domain_size-1] → 'de'
#   territory_id = abs(hash(country_code))
#
# Join thẳng country_code với lower(alpha_2) từ stg_country
# vì TLD cuối LUÔN khớp alpha_2 lowercase (kiểm tra thực tế).
# Ví dụ: com.au → 'au', co.uk → 'uk', com.ph → 'ph'
#
# Trường hợp đặc biệt: 'com' không có trong alpha_2
# → territory_id = -1, row unknown được thêm thủ công
# ============================================================
def build_dim_territory(spark):
    print("[DIM_TERRITORY] Đọc stg_events + stg_country...")

    stg_events  = read_ch(spark, "stg_events")
    stg_country = read_ch(spark, "stg_country")

    # --- Parse country_code theo đúng logic gốc ---
    territory_raw = (
        stg_events
        .filter(col("current_url").isNotNull())
        .withColumn("_domain",
            split(col("current_url"), "/").getItem(2))
        .withColumn("_domain_size",
            size(split(col("_domain"), r"\.")))
        .withColumn("country_code",
            # Lấy phần tử cuối sau khi split theo '.'
            # Tương đương logic gốc: split(domain,'.').getItem(domain_size-1)
            split(col("_domain"), r"\.").getItem(
                size(split(col("_domain"), r"\."))-1
            )
        )
        # territory_id: 'com' và null → -1, còn lại → abs(hash)
        .withColumn("territory_id",
            when(
                col("country_code").isNull()
                | (trim(col("country_code")) == "")
                | (trim(col("country_code")) == "com"),
                lit(-1).cast(LongType())
            ).otherwise(
                spark_abs(spark_hash(col("country_code"))).cast(LongType())
            )
        )
        .select("territory_id", "country_code")
        .dropDuplicates(["territory_id"])
        .filter(col("territory_id") != -1)   # bỏ -1 ở đây, sẽ union row chuẩn sau
    )

    # --- Chuẩn hoá stg_country: alpha_2 lowercase để join trực tiếp ---
    # Không cần mapping đặc biệt vì TLD cuối đã khớp alpha_2 lowercase
    stg_country_norm = (
        stg_country
        .withColumn("alpha_2_lower", F.lower(col("alpha_2")))
        .select(
            col("alpha_2_lower"),
            col("name").alias("country_name"),
            col("alpha_2"),
            col("alpha_3"),
            col("region"),
            col("sub_region"),
            col("intermediate_region"),
        )
    )

    # --- Join: country_code = alpha_2_lower ---
    dim_territory = (
        territory_raw
        .join(stg_country_norm,
              territory_raw["country_code"] == stg_country_norm["alpha_2_lower"],
              how="left")
        .select(
            territory_raw["territory_id"],
            territory_raw["country_code"],
            col("country_name"),
            col("alpha_2"),
            col("alpha_3"),
            col("region"),
            col("sub_region"),
            col("intermediate_region"),
        )
    )

    # --- Thêm row đặc biệt territory_id = -1 ---
    unknown_schema = dim_territory.schema
    unknown_row = spark.createDataFrame([
        Row(
            territory_id        = -1,
            country_code        = "com",
            country_name        = "Global / Unknown",
            alpha_2             = None,
            alpha_3             = None,
            region              = None,
            sub_region          = None,
            intermediate_region = None,
        )
    ], schema=unknown_schema)

    dim_territory = dim_territory.union(unknown_row)

    print(f"[DIM_TERRITORY] {dim_territory.count():,} territory (gồm 1 row unknown=-1).")
    return dim_territory


# ============================================================
# 3. DIM_PRODUCT
# ============================================================
def build_dim_product(spark):
    print("[DIM_PRODUCT] Đọc stg_product...")
    df = (
        read_ch(spark, "stg_product")
        .filter(col("product_id").isNotNull())
        .withColumn("product_id",
            col("product_id").cast(LongType()))
        .withColumn("product_name",
            when(
                col("product_name").isNull() | (trim(col("product_name")) == ""),
                lit(None)
            ).otherwise(trim(col("product_name")))
        )
        .select("product_id", "product_name")
        .dropDuplicates(["product_id"])
    )
    print(f"[DIM_PRODUCT] {df.count():,} sản phẩm.")
    return df


# ============================================================
# 4. DIM_DEVICE
# ============================================================
@udf(returnType=StringType())
def parse_os_udf(ua: str):
    if not ua:
        return None
    try:
        return user_agent_parser.ParseOS(ua).get("family")
    except Exception:
        return None


@udf(returnType=StringType())
def parse_browser_udf(ua: str):
    if not ua:
        return None
    try:
        return user_agent_parser.ParseUserAgent(ua).get("family")
    except Exception:
        return None


def infer_device_type(res_col):
    width = split(res_col, "x").getItem(0).cast(IntegerType())
    return (
        when(res_col.isNull(),  lit(None))
        .when(width < 768,      lit("Mobile"))
        .when(width < 1024,     lit("Tablet"))
        .otherwise(             lit("Desktop"))
    )


def build_dim_device(spark):
    print("[DIM_DEVICE] Đọc stg_events, parse user_agent...")
    df = (
        read_ch(spark, "stg_events")
        .filter(col("device_id").isNotNull())
        .filter(trim(col("device_id")) != "")
        .orderBy(col("event_time").desc())
        .dropDuplicates(["device_id"])
        .select("device_id", "user_agent", "resolution")
        .withColumn("os",          parse_os_udf(col("user_agent")))
        .withColumn("browser",     parse_browser_udf(col("user_agent")))
        .withColumn("device_type", infer_device_type(col("resolution")))
        .select("device_id", "user_agent", "os", "browser", "device_type", "resolution")
    )
    print(f"[DIM_DEVICE] {df.count():,} thiết bị unique.")
    return df


# ============================================================
# MAIN
# ============================================================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    write_ch(build_dim_date(spark),      "dim_date")
    write_ch(build_dim_territory(spark), "dim_territory")
    write_ch(build_dim_product(spark),   "dim_product")
    write_ch(build_dim_device(spark),    "dim_device")

    spark.stop()
    print("[DONE] Tất cả dim hoàn tất.")


if __name__ == "__main__":
    main()