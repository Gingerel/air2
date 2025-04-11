
USE popo;

select count(*)
from order_items;

select *
from order_items;


CREATE TABLE order_items (
    order_id VARCHAR(50),
    order_item_id VARCHAR(10),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price VARCHAR(20),
    freight_value VARCHAR(20)
);

CREATE TABLE geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat VARCHAR(20),
    geolocation_lng VARCHAR(20),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10)
);

CREATE TABLE order_payments (
    order_id VARCHAR(50),
    payment_sequential VARCHAR(10),
    payment_type VARCHAR(30),
    payment_installments VARCHAR(10),
    payment_value VARCHAR(20)
);

CREATE TABLE order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score VARCHAR(5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date VARCHAR(20),
    review_answer_timestamp VARCHAR(20)
);

CREATE TABLE orders_dataset (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp VARCHAR(20),
    order_approved_at VARCHAR(20),
    order_delivered_carrier_date VARCHAR(20),
    order_delivered_customer_date VARCHAR(20),
    order_estimated_delivery_date VARCHAR(20)
);

CREATE TABLE sellers_dataset (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

CREATE TABLE products_dataset (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_lenght VARCHAR(10),
    product_description_lenght VARCHAR(10),
    product_photos_qty VARCHAR(10),
    product_weight_g VARCHAR(10),
    product_length_cm VARCHAR(10),
    product_height_cm VARCHAR(10),
    product_width_cm VARCHAR(10)
);

CREATE TABLE product_category_name (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);


CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);


-- 일시적으로 STRICT 모드 해제
SET sql_mode = '';

UPDATE order_items
SET shipping_limit_date = NULL
WHERE shipping_limit_date = '0000-00-00 00:00:00';



DROP VIEW IF EXISTS rfm_customer_mart;



CREATE VIEW rfm_customer_mart AS
SELECT
    o.customer_id,

    -- 최근 구매일 기준 Recency (기준일은 데이터셋 내 가장 마지막 구매일)
    DATEDIFF(
        (SELECT MAX(order_purchase_timestamp) FROM orders_dataset),
        MAX(o.order_purchase_timestamp)
    ) AS recency,

    -- 주문 수 (Frequency)
    COUNT(DISTINCT o.order_id) AS frequency,

    -- 총 결제 금액 (price + 배송)
    ROUND(SUM(oi.price + oi.freight_value), 2) AS monetary,

    -- 평균 배송 소요 일수
    ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 2) AS avg_delivery_days,

    -- 평균 리뷰 점수
    ROUND(AVG(r.review_score), 2) AS avg_review_score,

    -- 총 리뷰 수 (참고용)
    COUNT(DISTINCT r.review_id) AS total_reviews

FROM orders_dataset o

LEFT JOIN order_items oi
    ON o.order_id = oi.order_id

LEFT JOIN order_reviews r
    ON o.order_id = r.order_id

GROUP BY o.customer_id;

SELECT *
FROM rfm_customer_mart;



CREATE VIEW rfm_customer_mart AS
SELECT
    o.customer_id,

    -- 최근 구매일 기준 Recency (기준일은 데이터셋 내 가장 마지막 구매일)
    DATEDIFF(
        (SELECT MAX(order_purchase_timestamp) FROM orders_dataset),
        MAX(o.order_purchase_timestamp)
    ) AS recency,

    -- 주문 수 (Frequency)
    COUNT(DISTINCT o.order_id) AS frequency,

    -- 총 결제 금액 (price + 배송)
    ROUND(SUM(oi.price + oi.freight_value), 2) AS monetary,

    -- 평균 배송 소요 일수
    ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 2) AS avg_delivery_days,

    -- 평균 리뷰 점수
    ROUND(AVG(r.review_score), 2) AS avg_review_score,

    -- 총 리뷰 수 (참고용)
    COUNT(DISTINCT r.review_id) AS total_reviews

FROM orders_dataset o

LEFT JOIN order_items oi
    ON o.order_id = oi.order_id

LEFT JOIN order_reviews r
    ON o.order_id = r.order_id

GROUP BY o.customer_id;



SELECT *
FROM rfm_customer_mart
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/rfm_customer_mart3.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n';







