
DROP VIEW IF EXISTS glamira_dw.v_revenue_detail;
select cuont(
CREATE VIEW glamira_dw.v_revenue_detail AS
SELECT
    -- 1. Identifiers
    fo.event_id         AS event_id,
    fo.order_id         AS order_id,
    fo.user_id_db       AS user_id_db,

    -- 2. Time Dimension
    dd.full_date        AS full_date,
    dd.year             AS year,
    dd.month            AS month,
    dd.day_of_month     AS day_of_month,
    dd.hour             AS hour,
    dd.day_of_week      AS day_of_week,

    -- 3. Default Geography/Territory Dimension (Giữ nguyên cấu trúc cột gốc)
    dt.territory_id     AS territory_id,
    dt.country_code     AS country_code,     
    dt.country_name     AS country_name,     
    dt.region           AS region,
    dt.sub_region       AS sub_region,

    -- 4. IP Location Dimension 
    di.ip               AS ip_address,
    di.country_code     AS ip_country_code,
    di.country_name     AS ip_country_name,
    di.region_name      AS ip_region_name,
    di.city_name        AS ip_city_name,
    di.latitude         AS ip_latitude,
    di.longitude        AS ip_longitude,
    di.time_zone        AS ip_time_zone,

    -- 5. Product & Jewelry Materials (Sạch sẽ, không dính lỗi alias)
    dp.product_id       AS product_id,       
    dp.product_name     AS product_name,
    ma.value_label      AS alloy_name,
    md.value_label      AS diamond_name,

    -- 6. Device & Browser Dimension
    dv.device_type      AS device_type,
    dv.os               AS os,
    dv.browser          AS browser,

    -- 7. Currency
    dc.currency_code    AS currency_code,
    dc.usd_conversion_rate AS usd_conversion_rate,

    -- 8. Metrics (Đảm bảo độ chính xác tuyệt đối sau khi khử trùng)
    fo.amount           AS amount,
    fo.price_local      AS price_local,
    fo.price_usd        AS price_usd
FROM glamira_dw.fact_order fo

-- 🛡️ Khử trùng triệt để trên tất cả các trục Dimension bằng LIMIT 1 BY
LEFT JOIN (SELECT * FROM glamira_dw.dim_date LIMIT 1 BY date_id) dd 
    ON fo.date_id = dd.date_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_territory LIMIT 1 BY territory_id) dt 
    ON fo.territory_id = dt.territory_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_ip LIMIT 1 BY ip_id) di 
    ON fo.ip_id = di.ip_id          

LEFT JOIN (SELECT * FROM glamira_dw.dim_product LIMIT 1 BY product_id) dp 
    ON fo.product_id = dp.product_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_material LIMIT 1 BY material_id) ma 
    ON fo.alloy_id = ma.material_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_material LIMIT 1 BY material_id) md 
    ON fo.diamond_id = md.material_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_device LIMIT 1 BY device_id) dv 
    ON fo.device_id = dv.device_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_currency LIMIT 1 BY currency_id) dc 
    ON fo.currency_id = dc.currency_id;
SELECT 
    COUNT(event_id) AS "Tổng lượt tương tác",
    COUNT(DISTINCT user_id_db) AS "Tổng số người dùng"
FROM glamira_dw.v_behavior_detail;

SELECT product_id, COUNT() FROM glamira_dw.dim_product GROUP BY product_id HAVING COUNT() > 1 LIMIT 5;
SELECT ip_id, COUNT() FROM glamira_dw.dim_ip GROUP BY ip_id HAVING COUNT() > 1 LIMIT 5;
SELECT territory_id, COUNT() FROM glamira_dw.dim_territory GROUP BY territory_id HAVING COUNT() > 1 LIMIT 5;
SELECT device_id, COUNT() FROM glamira_dw.dim_device GROUP BY device_id HAVING COUNT() > 1 LIMIT 5;

DROP VIEW IF EXISTS glamira_dw.v_behavior_detail;

CREATE VIEW glamira_dw.v_behavior_detail AS
SELECT
    -- 1. Thông tin Sự kiện Core (Event Metrics)
    fe.event_id         AS event_id,
    fe.collection       AS event_action, 
    fe.current_url      AS current_url,
    fe.event_count      AS event_count,
    fe.store_id         AS store_id,
    fe.user_id_db       AS user_id_db,

    -- 2. Trục Thời gian (Time)
    dd.full_date        AS full_date,
    dd.year             AS year,
    dd.month            AS month,
    dd.day_of_month     AS day_of_month,
    dd.hour             AS hour,
    dd.day_of_week      AS day_of_week,

    -- 3. Trục Địa lý bóc từ IP (IP-Geo Dimensions - Đã khử trùng)
    di.ip               AS ip_address,
    di.country_code     AS ip_country_code,
    di.country_name     AS ip_country_name,
    di.region_name      AS ip_region_name,
    di.city_name        AS ip_city_name,
    di.time_zone        AS ip_time_zone,

    -- 4. Trục Không gian Địa lý Quốc gia (Territory Dimensions từ bảng cũ - Đã khử trùng)
    dt.territory_id     AS territory_id,
    dt.country_code     AS territory_country_code,
    dt.country_name     AS territory_country_name,
    dt.region           AS territory_region,
    dt.sub_region       AS territory_sub_region,

    -- 5. Trục Thiết bị sử dụng (Device Dimensions - Đã khử trùng)
    dv.device_id        AS device_id,
    dv.device_type      AS device_type,
    dv.os               AS os,
    dv.browser          AS browser,
    dv.resolution       AS resolution,

    -- 6. Trục Sản phẩm liên kết (Product Dimensions - Đã khử trùng)
    dp.product_id       AS product_id,
    IF(dp.product_name IS NULL, 'Xem trang chung (Không có SP)', dp.product_name) AS product_name

FROM glamira_dw.fact_events fe

-- Ép các bảng Dim chỉ lấy 1 dòng duy nhất cho mỗi ID để chống bùng nổ data (Fan-out)
LEFT JOIN (SELECT * FROM glamira_dw.dim_date LIMIT 1 BY date_id) dd 
    ON fe.date_id = dd.date_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_ip LIMIT 1 BY ip_id) di 
    ON fe.ip_id = di.ip_id          

LEFT JOIN (SELECT * FROM glamira_dw.dim_territory LIMIT 1 BY territory_id) dt 
    ON fe.territory_id = dt.territory_id   

LEFT JOIN (SELECT * FROM glamira_dw.dim_device LIMIT 1 BY device_id) dv 
    ON fe.device_id = dv.device_id

LEFT JOIN (SELECT * FROM glamira_dw.dim_product LIMIT 1 BY product_id) dp 
    ON fe.product_id = dp.product_id;
SELECT *
FROM glamira_dw.v_revenue_detail 
LIMIT 10;

SELECT ip_id, COUNT() FROM glamira_dw.dim_ip GROUP BY ip_id HAVING COUNT() > 1 ;