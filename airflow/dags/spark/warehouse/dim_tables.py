import os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col, split, size, abs as spark_abs, hash as spark_hash,
    date_format, dayofmonth, month, year, hour,
    udf, trim, when, lit, explode, from_json, regexp_replace
)
from pyspark.sql.types import ArrayType, StructType, StructField
from ua_parser import user_agent_parser

CLICKHOUSE_HOST     = os.environ.get("CLICKHOUSE_HOST",     "127.0.0.1")
CLICKHOUSE_PORT     = os.environ.get("CLICKHOUSE_PORT",     "8123")
CLICKHOUSE_DW       = os.environ.get("CLICKHOUSE_DW",       "glamira_dw")
CLICKHOUSE_STG      = os.environ.get("CLICKHOUSE_STG",      "glamira_stg")
CLICKHOUSE_USER     = os.environ.get("CLICKHOUSE_USER",     "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "123")

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Glamira_Build_Dim")
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
# HELPER (Tách biệt rõ ràng URL Staging để Đọc và Data Warehouse để Ghi)
# ============================================================
URL_READ_STG  = "jdbc:clickhouse://127.0.0.1:8123/glamira_stg?http_connection_provider=HTTP_URL_CONNECTION"
URL_WRITE_DW  = "jdbc:clickhouse://127.0.0.1:8123/glamira_dw?http_connection_provider=HTTP_URL_CONNECTION"

CLICKHOUSE_USER     = "admin"
CLICKHOUSE_PASSWORD = "123"

def read_ch(spark, table_name):
    print(f"[READ] Đang đọc dữ liệu từ ClickHouse table: {table_name}...")
    return spark.read \
        .format("jdbc") \
        .option("url",      URL_READ_STG) \
        .option("dbtable",  table_name) \
        .option("user",     CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
        .load()

def write_ch(df, table_name):
    print(f"[WRITE] Đang chuẩn bị xử lý và ghi vào ClickHouse DW: {table_name}...")
    
    # Lấy tên cột đầu tiên làm khóa sắp xếp mặc định nếu bảng chưa tồn tại
    first_column = df.columns[0]
    
    df.write \
      .format("jdbc") \
      .option("url",      URL_WRITE_DW) \
      .option("dbtable",  table_name) \
      .option("user",     CLICKHOUSE_USER) \
      .option("password", CLICKHOUSE_PASSWORD) \
      .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
      .option("batchsize", "20000") \
      .option("isolationLevel", "NONE") \
      .option("rewriteBatchedStatements", "true") \
      .option("createTableOptions", f"ENGINE = MergeTree() ORDER BY {first_column}") \
      .mode("append") \
      .save()


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


def build_dim_territory(spark):
    print("[DIM_TERRITORY] Đọc stg_events + stg_country...")

    stg_events  = read_ch(spark, "stg_events")
    stg_country = read_ch(spark, "stg_country")

    # --- Parse TLD theo đúng logic gốc ---
    territory_raw = (
        stg_events
        .filter(col("current_url").isNotNull())
        .withColumn("_domain",
            split(col("current_url"), "/").getItem(2))
        .withColumn("_domain_size",
            size(split(col("_domain"), r"\.")))
        .withColumn("country_code",
            split(col("_domain"), r"\.").getItem(
                size(split(col("_domain"), r"\."))-1
            )
        )
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
        .filter(col("territory_id") != -1)
    )

    # --- Join thẳng alpha_2 lowercase — không cần mapping ---
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
    ], schema=dim_territory.schema)

    dim_territory = dim_territory.union(unknown_row)
    print(f"[DIM_TERRITORY] {dim_territory.count():,} territory (gồm 1 row unknown=-1).")
    return dim_territory


# ============================================================
# 3. DIM_PRODUCT
# ============================================================
def build_dim_product(spark):
    print("[DIM_PRODUCT] Đọc stg_product...")
    df = (
        read_ch(spark, "stg_products")
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



# Schema parse option_json
OPTION_SCHEMA = ArrayType(StructType([
    StructField("option_label", StringType(), True),
    StructField("option_id",    StringType(), True),
    StructField("value_label",  StringType(), True),
    StructField("value_id",     StringType(), True),
    StructField("alloy",        StringType(), True),
    StructField("diamond",      StringType(), True),
    StructField("stone",        StringType(), True),
    StructField("quality",      StringType(), True),
    StructField("quality_label",StringType(), True),
    StructField("finish",       StringType(), True),
    StructField("pearlcolor",   StringType(), True),
]))

# Schema parse cart_products_json
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


def build_dim_material(spark):
    print("[DIM_MATERIAL] Đọc option_json + cart_products_json từ stg_events...")

    stg = read_ch(spark, "stg_events")


    # ----------------------------------------------------------
    # Nguồn 2: cart_products_json (nested array)
    # Explode cart → explode option bên trong
    # ----------------------------------------------------------
    src = (
        stg
        .filter(col("cart_products_json").isNotNull())
        .withColumn("_carts",
            from_json(col("cart_products_json"), CART_PRODUCT_SCHEMA))
        .withColumn("_cart", explode(col("_carts")))
        .withColumn("_opt",  explode(col("_cart.option")))
        .select(
            F.lower(trim(col("_opt.option_label"))).alias("material_type"),
            trim(col("_opt.value_label")).alias("value_label"),
        )
    )

    # ----------------------------------------------------------
    # Gộp 2 nguồn, chỉ giữ alloy và diamond, loại bỏ giá trị rỗng
    # ----------------------------------------------------------
    dim_material = (
        src
        .filter(col("material_type").isin("alloy", "diamond"))
        .filter(col("value_label").isNotNull())
        .filter(trim(col("value_label")) != "")
        .withColumn("material_id",
            spark_abs(spark_hash(col("value_label"))).cast(LongType()))
        .select("material_id", "material_type", "value_label")
        .dropDuplicates(["material_id"])
        .orderBy("material_type", "value_label")
    )

    print(f"[DIM_MATERIAL] {dim_material.count():,} nguyên liệu unique (alloy + diamond).")
    return dim_material

# ============================================================
# 6. DIM_CURRENCY 
# Xử lý chuẩn hóa và gán tỷ giá chuyển đổi quy về USD
# ============================================================
def build_dim_currency(spark):
    print("[DIM_CURRENCY] Khởi tạo danh sách tỷ giá currency...")
    
    stg_events = read_ch(spark, "stg_events")
    
    raw_currency = (
        stg_events
        .filter(col("currency").isNotNull())
        .withColumn("currency_clean", trim(regexp_replace(col("currency"), r"[\u00a0\s]+", " ")))
        .filter(col("currency_clean") != "")
        .select("currency_clean")
        .distinct()
    )
    
    dim_currency = (
        raw_currency
        .withColumn("currency_id", spark_abs(spark_hash(col("currency_clean"))).cast(LongType()))
        .withColumn("usd_conversion_rate",
            # --- Nhóm EURO (€) ---
            when(col("currency_clean").isin("€", "EUR", "euro", "евро", "يورو"), lit(1.10))
            
            # --- Nhóm USD ($) ---
            .when(col("currency_clean").isin("$", "USD", "US$", "USD $", "US $", "dolar", "דולר", "$US", "долл США"), lit(1.00))
            
            # --- Nhóm Bảng Anh (£) ---
            .when(col("currency_clean").isin("£", "GBP"), lit(1.24))
            
            # --- Nhóm Đô la Úc (AUD) ---
            .when(col("currency_clean").isin("AU $", "AUD $", "AUD"), lit(0.67))
            
            # --- Nhóm Đô la Canada (CAD) ---
            .when(col("currency_clean").isin("CAD $", "加币$", "加元", "CAD", "$ CAD"), lit(0.75))
            
            # --- Nhóm Thụy Sĩ (CHF) ---
            .when(col("currency_clean").isin("CHF", "CHF '", "швейцарских франka", "швейцарских франков", "швейцарских франка"), lit(1.04))
            
            # --- Nhóm Đô la Hồng Kông (HKD) ---
            .when(col("currency_clean").isin("HKD $", "港币$", "HKD"), lit(0.13))
            
            # --- Nhóm Yên Nhật / Nhân Dân Tệ (￥/¥) ---
            .when(col("currency_clean").isin("￥", "¥", "JPY", "CNY"), lit(0.0070))
            
            # --- Nhóm Đô la Đài Loan (TWD) ---
            .when(col("currency_clean").isin("NT$", "TWD"), lit(0.033))
            
            # --- Nhóm Tiền tệ Đông Nam Á ---
            .when(col("currency_clean").isin("₫", "VND"), lit(0.000042)) 
            .when(col("currency_clean").isin("฿", "THB"), lit(0.031))   
            .when(col("currency_clean").isin("RM", "MYR"), lit(0.23))   
            .when(col("currency_clean").isin("Rp", "IDR"), lit(0.000067)) 
            .when(col("currency_clean").isin("₱", "PHP"), lit(0.020))   
            .when(col("currency_clean").isin("SGD $", "SGD"), lit(0.71)) 
            
            # --- Nhóm Tiền tệ Châu Âu khác ---
            .when(col("currency_clean").isin("zł", "PLN", "злотых", "зл"), lit(0.25)) # Ba Lan
            .when(col("currency_clean").isin("Kč", "CZK"), lit(0.041))   # Séc
            .when(col("currency_clean").isin("Ft", "HUF"), lit(0.0031))  # Hungary
            .when(col("currency_clean").isin("Lei", "RON"), lit(0.23))   # Romania
            .when(col("currency_clean").isin("лв.", "лв", "BGN"), lit(0.56)) # Bulgaria
            .when(col("currency_clean").isin("din.", "din", "RSD"), lit(0.0094)) # Serbia
            .when(col("currency_clean").isin("kn", "HRK"), lit(0.15))    # Croatia
            .when(col("currency_clean").isin("Lekë", "ALL"), lit(0.0091)) # Albania
            .when(col("currency_clean").isin("kr", "SEK", "瑞典克朗", "шведских крон", "، كرونة"), lit(0.10)) # Thụy Điển
            .when(col("currency_clean").isin("₴", "UAH"), lit(0.037))    # Ukraina
            
            # --- Nhóm Tiền tệ Châu Mỹ ---
            .when(col("currency_clean").isin("MXN $", "MXN"), lit(0.045)) # Mexico
            .when(col("currency_clean") == "CLP", lit(0.0013))          # Chile
            .when(col("currency_clean") == "CRC ₡", lit(0.0018))        # Costa Rica
            .when(col("currency_clean").isin("PEN S/.", "PEN S/", "PEN S"), lit(0.27)) # Peru
            .when(col("currency_clean") == "GTQ Q", lit(0.13))          # Guatemala
            .when(col("currency_clean") == "BOB Bs", lit(0.14))         # Bolivia
            .when(col("currency_clean") == "UYU", lit(0.023))           # Uruguay
            .when(col("currency_clean") == "DOP $", lit(0.018))         # CH Dominica
            .when(col("currency_clean") == "R$", lit(0.19))             # Brazil
            .when(col("currency_clean") == "₲", lit(0.00015))           # Paraguay
            .when(col("currency_clean").isin("COP $", "COP"), lit(0.00026)) # Colombia
            .when(col("currency_clean") == "HNL L", lit(0.040))         # Honduras
            
            # --- Nhóm Châu Phi & Trung Đông ---
            .when(col("currency_clean") == "ZAR", lit(0.056))           # Nam Phi
            .when(col("currency_clean") == "KMF", lit(0.0022))          # Comoros
            .when(col("currency_clean").isin("د.ك.‏", "د ك", "KWD"), lit(3.27)) # Kuwait
            .when(col("currency_clean").isin("AED", "، درهم"), lit(0.27)) # UAE
            .when(col("currency_clean") == "AZN", lit(0.59))            # Azerbaijan
            .when(col("currency_clean") == "₺", lit(0.11))              # Thổ Nhĩ Kỳ
            .when(col("currency_clean") == "AFN", lit(0.013))           # Afghanistan
            
            # --- Nhóm Châu Á khác ---
            .when(col("currency_clean").isin("₩", "KRW"), lit(0.00083)) # Hàn Quốc
            .when(col("currency_clean").isin("руб.", "RUB"), lit(0.013)) # Nga
            .when(col("currency_clean").isin("₹", "INR"), lit(0.013))   # Ấn Độ
            
            # --- Các trường hợp đặc biệt ---
            .when(col("currency_clean") == "Ucretsiz", lit(0.00))       # Miễn phí
            .when(col("currency_clean").isin("NZD $", "NZD"), lit(0.61)) # New Zealand
            
            .otherwise(lit(None).cast(DoubleType()))
        )
        .select(
            col("currency_id"),
            col("currency_clean").alias("currency_code"),
            col("usd_conversion_rate")
        )
    )

    unknown_row = spark.createDataFrame([
        Row(
            currency_id        = -1,
            currency_code      = "Unknown",
            usd_conversion_rate = None
        )
    ], schema=dim_currency.schema)

    dim_currency = dim_currency.union(unknown_row).dropDuplicates(["currency_id"])
    print(f"[DIM_CURRENCY] Đã sinh xong {dim_currency.count():,} loại đơn vị tiền tệ.")
    return dim_currency
# ============================================================
# MAIN
# ============================================================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    write_ch(build_dim_date(spark),      "dim_date")
    # write_ch(build_dim_territory(spark), "dim_territory")
    # write_ch(build_dim_product(spark),   "dim_product")
    # write_ch(build_dim_device(spark),    "dim_device")
    # write_ch(build_dim_material(spark),  "dim_material")


    # write_ch(build_dim_currency(spark),  "dim_currency")
    spark.stop()
    print("[DONE] Tất cả dim hoàn tất.")


if __name__ == "__main__":
    main()