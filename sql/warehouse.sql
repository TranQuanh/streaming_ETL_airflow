select cart_products_json, collection from glamira_stg.stg_events where option_json is not null limit 1000

DROP TABLE IF EXISTS glamira_dwh.dim_date;
DROP TABLE IF EXISTS glamira_dwh.dim_territory;
DROP TABLE IF EXISTS glamira_dwh.dim_product;
DROP TABLE IF EXISTS glamira_dwh.dim_device;
CREATE DATABASE IF NOT EXISTS glamira_dwh;
use glamira_dwh;
-- ------------------------------------------------------------
-- 1. dim_date
-- Sinh từ sequence timestamp, granularity theo giờ
-- date_id = HHddMMyyyy (Long) — unique mỗi giờ
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glamira_dw.dim_date (
    date_id           Int64,       -- HHddMMyyyy, PK
    full_date         DateTime,
    day_of_week       String,      -- Monday, Tuesday...
    day_of_week_short String,      -- Mon, Tue...
    day_of_month      Int32,       -- 1-31
    month             Int32,       -- 1-12
    year              Int32,
    hour              Int32        -- 0-23
)
ENGINE = MergeTree()
ORDER BY (year, month, day_of_month, hour)
COMMENT 'Dimension thời gian, granularity theo giờ, range 2020-01-01 đến 2020-12-31';
 
 
-- ------------------------------------------------------------
-- 2. dim_territory
-- territory_id = abs(hash(country_code)) từ TLD của current_url
-- Row đặc biệt: territory_id = -1 cho glamira.com và URL lỗi
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glamira_dw.dim_territory (
    territory_id        Int64,              -- PK, -1 = unknown/global
    country_code        String,             -- 'de', 'fr', 'au', 'com'...
    country_name        Nullable(String),
    alpha_2             Nullable(String),   -- ISO alpha-2: DE, FR, GB
    alpha_3             Nullable(String),   -- ISO alpha-3: DEU, FRA, GBR
    region              Nullable(String),   -- Europe, Asia...
    sub_region          Nullable(String),
    intermediate_region Nullable(String)
)
ENGINE = MergeTree()
ORDER BY territory_id
COMMENT 'Dimension địa lý. territory_id=-1 đại diện glamira.com (global) và URL không xác định';
 
 
-- ------------------------------------------------------------
-- 3. dim_product
-- Chỉ product_id + product_name, đẩy thẳng từ stg_product
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glamira_dw.dim_product (
    product_id   Int64,           -- PK
    product_name Nullable(String)
)
ENGINE = MergeTree()
ORDER BY product_id
COMMENT 'Dimension sản phẩm, nguồn từ stg_product';
 
 
-- ------------------------------------------------------------
-- 4. dim_device
-- OS và browser giữ trong cùng bảng, không tách riêng
-- ReplacingMergeTree tự deduplicate theo device_id
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glamira_dw.dim_device (
    device_id    String,           -- PK
    user_agent   Nullable(String),
    os           Nullable(String), -- Windows, iOS, Android, Mac OS X...
    browser      Nullable(String), -- Chrome, Safari, Firefox...
    device_type  Nullable(String), -- Mobile / Tablet / Desktop
    resolution   Nullable(String)  -- raw: '375x667'
)
ENGINE = ReplacingMergeTree()
ORDER BY device_id
COMMENT 'Dimension thiết bị, OS và browser parse từ user_agent';

-- ------------------------------------------------------------
-- 5. dim_material
-- Gộp alloy và diamond chung 1 bảng vì cùng cấu trúc
-- material_id = abs(hash(value_label)) — join từ fact
-- Nguồn: option_json và cart_products_json từ stg_events
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glamira_dw.dim_material (
    material_id   Int64,   -- abs(hash(value_label)), PK
    material_type String,  -- 'alloy' hoặc 'diamond'
    value_label   String   -- 'Gelbgold 585', 'White Sapphire'...
)
ENGINE = MergeTree()
ORDER BY (material_type, material_id)
COMMENT 'Dimension nguyên liệu trang sức, gộp alloy và diamond, join qua material_id = abs(hash(value_label))';
 