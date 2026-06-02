select * from glamira_stg.stg_events 
where collection = 'view_product_detail' 
and event_id = '5ed8cb2bc671fc36b74653ad'limit 1000;
DROP TABLE IF EXISTS glamira_stg.stg_events;
DROP TABLE IF EXISTS glamira_stg.stg_products;
DROP TABLE IF EXISTS glamira_stg.stg_country;
DROP TABLE IF EXISTS glamira_stg.stg_ip;
-- 2. Tạo lại bảng Staging phân mảnh theo NGÀY (Đã sửa kiểu dữ liệu)
USE glamira_stg;

CREATE TABLE IF NOT EXISTS glamira_stg.stg_events (
    event_id String,
    event_time DateTime64(3),
    local_time Nullable(DateTime),   
    collection String,
    ip Nullable(String),
    user_agent Nullable(String),
    resolution Nullable(String),
    user_id_db Nullable(String),
    device_id Nullable(String),
    store_id Nullable(String),       -- NẾU Spark bạn thêm cast Integer thì sửa thành Nullable(Int32)
    email_address Nullable(String),
    current_url Nullable(String),
    referrer_url Nullable(String),
    
    -- 🔥 ĐÃ SỬA: Ép về kiểu Bool khớp với Spark BooleanType
    utm_source Nullable(Bool),       
    utm_medium Nullable(Bool),       
    
    key_search Nullable(String),
    api_version Nullable(String),
    
    -- 🔥 ĐÃ SỬA: Ép về kiểu số nguyên khớp với Spark IntegerType
    product_id Nullable(Int32),      
    
    cat_id Nullable(String),         -- NẾU Spark bạn thêm cast Integer thì sửa thành Nullable(Int32)
    collect_id Nullable(String),
    order_id Nullable(String),
    price Nullable(Decimal(18, 2)), 
    currency Nullable(String),
    
    -- 🔥 ĐÃ SỬA: Int8 thành Bool cho đồng bộ với Spark
    is_paypal Nullable(Bool),        
    recommendation Nullable(Bool),   
    
    show_recommendation Nullable(String),
    
    -- CÁC TRƯỜNG MỚI ĐÃ KHỚP CHUẨN
    recommendation_product_id Nullable(String),
    recommendation_product_position Nullable(Int32),
    recommendation_clicked_position Nullable(Int32),
    
    option_json Nullable(String),
    cart_products_json Nullable(String)
) 
ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (collection, event_id, event_time)
SETTINGS index_granularity = 8192;


-- 1. Bảng Staging Country (Đã chuyển dấu gạch ngang thành gạch dưới)
CREATE TABLE IF NOT EXISTS glamira_stg.stg_country (
    name String,
    alpha_2 String,
    alpha_3 String,
    country_code String,
    iso_3166_2 String,
    region String,
    sub_region String,
    intermediate_region Nullable(String),
    region_code String,
    sub_region_code String,
    intermediate_region_code Nullable(String),
    updated_at DateTime DEFAULT now()
) 
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY country_code;

-- 2. Bảng Staging Products (Cấu trúc chuẩn hóa phẳng để map với Fact)
CREATE TABLE IF NOT EXISTS glamira_stg.stg_products (
    product_id Int32,
    product_name String,
    updated_at DateTime DEFAULT now()
) 
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;
CREATE TABLE IF NOT EXISTS glamira_stg.stg_ip (
    ip String,
    country_code String,
    country_name String,
    region_name String,
    city_name String,
    latitude String,
    longitude String,
    zip_code String,
    time_zone String,
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY ip;
select * from glamira_stg.stg_products

select * from glamira_stg.stg_country