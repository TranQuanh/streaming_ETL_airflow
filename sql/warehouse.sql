-- ============================================================================
-- KHỞI TẠO DATABASE
-- ============================================================================
CREATE DATABASE IF NOT EXISTS glamira_dw;
DROP TABLE IF EXISTS glamira_dw.fact_events;
DROP TABLE IF EXISTS glamira_dw.fact_order;
USE glamira_dw;
TRUNCATE TABLE glamira_dw.dim_territory
TRUNCATE TABLE glamira_dw.dim_ip
TRUNCATE TABLE glamira_dw.fact_order
TRUNCATE TABLE glamira_dw.fact_events
	select * from glamira_dw.dim_currency
select * from glamira_dw.fact_events
select * from glamira_stg.stg_events where event_id = '5e857b5f5d4dd036fab55dc7';
select count(*) from  glamira_dw.fact_order
-- ============================================================================
-- A. KHỐI BẢNG CHIỀU THÔNG TIN (DIMENSION TABLES)
-- ============================================================================
SELECT 
    toHour(toDateTime(event_time)) AS hour_stg,
    COUNT() AS total_rows
FROM glamira_stg.stg_events
WHERE hour_stg BETWEEN 6 AND 16
GROUP BY hour_stg
ORDER BY hour_stg ASC;
-- 1. Bảng dim_date (Trục Thời gian Phân tích)
CREATE TABLE IF NOT EXISTS glamira_dw.dim_date
(
    date_id Int64,
    full_date DateTime,
    day_of_week String,
    day_of_week_short String,
    day_of_month Int32,
    month Int32,
    year Int32,
    hour Int32
)
ENGINE = MergeTree()
ORDER BY date_id;

-- 2. Bảng dim_territory (Chiều Không gian Địa lý Quốc gia)
CREATE TABLE IF NOT EXISTS glamira_dw.dim_territory
(
    territory_id Int64,
    country_code String,
    country_name Nullable(String),
    alpha_2 Nullable(String),
    alpha_3 Nullable(String),
    region Nullable(String),
    sub_region Nullable(String),
    intermediate_region Nullable(String)
)
ENGINE = MergeTree()
ORDER BY territory_id;

-- 3. Bảng dim_product (Chiều thông tin Sản phẩm)
CREATE TABLE IF NOT EXISTS glamira_dw.dim_product
(
    product_id Int64,
    product_name Nullable(String)
)
ENGINE = MergeTree()
ORDER BY product_id;

-- 4. Bảng dim_device (Chiều thông tin Thiết bị & Trình duyệt)
CREATE TABLE IF NOT EXISTS glamira_dw.dim_device
(
    device_id String,
    user_agent Nullable(String),
    os Nullable(String),
    browser Nullable(String),
    device_type Nullable(String),
    resolution Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY device_id;

-- 5. Bảng dim_material (Chiều thông tin Nguyên liệu Trang sức cao cấp)
CREATE TABLE IF NOT EXISTS glamira_dw.dim_material
(
    material_id Int64,
    material_type String,
    value_label String
)
ENGINE = MergeTree()
ORDER BY material_id;

-- 6. Bảng dim_currency (Bảng quy đổi Tỷ giá tiền tệ Quốc tế)
CREATE TABLE IF NOT EXISTS glamira_dw.dim_currency
(
    currency_id Int64,
    currency_code String,
    usd_conversion_rate Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY currency_id;

DROP TABLE IF EXISTS glamira_dw.dim_ip;
CREATE TABLE glamira_dw.dim_ip (
    ip_id Int64,
    ip String,
    

    -- 2. Trục Quốc gia
    country_code String,                    -- Ví dụ: VN, US
    country_name String,                    -- Ví dụ: Vietnam, United States
    
    -- 3. Các chi tiết địa lý nâng cao (Để Nullable theo đúng thiết kế Spark của bạn)
    region_name Nullable(String),           -- Tỉnh/Thành phố trực thuộc
    city_name Nullable(String),             -- Quận/Huyện/Thị xã
    latitude Nullable(String),              -- Vĩ độ
    longitude Nullable(String),             -- Kinh độ
    zip_code Nullable(String),              -- Mã bưu chính
    time_zone Nullable(String)              -- Múi giờ (Ví dụ: Asia/Ho_Chi_Minh)
) ENGINE = MergeTree()
ORDER BY ip_id;

-- ============================================================================
-- B. KHỐI BẢNG SỰ KIỆN CỐT LÕI (FACT TABLES)
-- ============================================================================

-- 1. Bảng fact_order (Báo cáo Doanh thu - Đơn hàng Chi tiết)
CREATE TABLE IF NOT EXISTS glamira_dw.fact_order
(
    event_id String,
    order_id String,
    date_id Int64,
    ip_id Int64,             -- Khóa ngoại liên kết trực tiếp với dim_ip
    territory_id Int64,      -- Có thể giữ lại nếu bạn vẫn dùng, hoặc xóa bỏ nếu bỏ hẳn stg_country
    device_id String,
    product_id Int64,
    currency_id Int64,
    alloy_id Int64,
    diamond_id Int64,
    user_id_db Nullable(Int64),
    show_recommendation UInt8,
    amount Int32,
    price_local Float64,
    price_usd Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (date_id, ip_id, product_id); -- Đổi khóa sắp xếp từ territory_id sang ip_id để tối ưu index


-- ============================================================
-- 2. Bảng fact_events (Đã thêm ip_id, ENGINE và ORDER BY hoàn chỉnh)
-- ============================================================
CREATE TABLE IF NOT EXISTS glamira_dw.fact_events
(
    event_id String,
    collection String,
    date_id Int64,
    ip_id Int64,             -- Khóa ngoại liên kết trực tiếp với dim_ip
    territory_id Int64,      -- Có thể giữ lại hoặc bỏ tùy thuộc kiến trúc của bạn
    device_id String,
    store_id String,
    user_id_db Nullable(String),
    product_id Int64,
    current_url String,
    utm_source String,
    utm_medium String,
    event_count Int32
)
ENGINE = MergeTree()
ORDER BY (date_id, ip_id, product_id); -- Bổ sung cấu hình MergeTree hoàn chỉnh cho bảng events