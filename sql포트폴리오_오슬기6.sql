
create database popo;

USE popo;
  

/****************************************************************************/
/*****************************분석용 데이터 마트 구성******************************/
/****************************************************************************/

/****고객 기반 분석****/
-- 데이터 마트 생성
CREATE OR REPLACE VIEW Customer_Analysis AS
SELECT 
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    oi.product_id,
    pcn.product_category_name_english,
    p.product_name_lenght as product_name_length,
    p.product_description_lenght as product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    oi.price,
    oi.freight_value,
    op.payment_type,
    op.payment_value,
    r.review_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
JOIN 
    order_items oi ON o.order_id = oi.order_id
JOIN 
    products p ON oi.product_id = p.product_id
JOIN 
    product_category_name pcn ON p.product_category_name = pcn.product_category_name
JOIN 
    order_payments op ON o.order_id = op.order_id
LEFT JOIN 
    reveiw r ON o.order_id = r.order_id;

SHOW VARIABLES LIKE 'secure_file_priv';



-- 데이터 마트 전체 조회 및 CSV로 내보내기
SELECT * 
FROM Customer_Analysis
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/customer_analysis.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- 지역별 고객 수 분석 및 CSV로 내보내기
SELECT customer_state, COUNT(DISTINCT customer_unique_id) AS num_customers
FROM customers
GROUP BY customer_state
ORDER BY num_customers DESC
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/num_customers_by_state.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- 평균 주문 가치 분석 및 CSV로 내보내기
SELECT o.order_id, AVG(oi.price) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id
ORDER BY avg_order_value DESC
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/avg_order_value.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- 제품별 판매량 분석 및 CSV로 내보내기
SELECT product_id, COUNT(*) AS total_sold
FROM order_items
GROUP BY product_id
ORDER BY total_sold DESC
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/total_sold_by_product.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- 고객별 주문 횟수와 총 지출 분석 및 CSV로 내보내기
SELECT 
    c.customer_unique_id,
    COUNT(o.order_id) AS total_orders,
    SUM(oi.price) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_unique_id
ORDER BY total_spent DESC
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/total_orders_and_spent_by_customer.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- 결제 유형별 선호도 분석 및 CSV로 내보내기
SELECT payment_type, COUNT(*) AS count, SUM(payment_value) AS total_value
FROM order_payments
GROUP BY payment_type
ORDER BY count DESC
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/payment_preference.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- RFM 분석 결과 및 CSV로 내보내기
WITH RFM AS (
    SELECT
        customer_unique_id,
        DATEDIFF(CURRENT_DATE, MAX(order_purchase_timestamp)) AS Recency,
        COUNT(DISTINCT order_id) AS Frequency,
        SUM(price) AS Monetary
    FROM
        Customer_Analysis
    GROUP BY
        customer_unique_id
),
RFM_Scores AS (
    SELECT
        customer_unique_id,
        Recency,
        Frequency,
        Monetary,
        NTILE(10) OVER (ORDER BY Recency) AS R_Score,
        NTILE(10) OVER (ORDER BY Frequency DESC) AS F_Score,
        NTILE(10) OVER (ORDER BY Monetary DESC) AS M_Score
    FROM RFM
),
RFM_Segments AS (
    SELECT
        customer_unique_id,
        R_Score,
        F_Score,
        M_Score,
        CASE
            WHEN R_Score BETWEEN 1 AND 3 THEN 'High'
            WHEN R_Score BETWEEN 4 AND 7 THEN 'Medium'
            ELSE 'Low'
        END AS Recency_Segment,
        CASE
            WHEN F_Score BETWEEN 1 AND 3 THEN 'Low'
            WHEN F_Score BETWEEN 4 AND 7 THEN 'Medium'
            ELSE 'High'
        END AS Frequency_Segment,
        CASE
            WHEN M_Score BETWEEN 1 AND 3 THEN 'Low'
            WHEN M_Score BETWEEN 4 AND 7 THEN 'Medium'
            ELSE 'High'
        END AS Monetary_Segment,
        R_Score + F_Score + M_Score AS RFM_Total_Score
    FROM RFM_Scores
)
SELECT *
FROM RFM_Segments
ORDER BY RFM_Total_Score DESC
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/rfm_segments.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- 리뷰 스코어에 따른 도착일과 주문 수 분석 및 CSV로 내보내기
SELECT
    review_score,
    AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) AS Avg_Arrival_Days,
    COUNT(order_id) AS Order_Count
FROM
    Customer_Analysis
GROUP BY
    review_score
ORDER BY
    review_score
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/avg_arrival_days_and_order_count_by_review_score.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- 리뷰 스코어별 주문 수 분석 및 CSV로 내보내기
SELECT
    review_score,
    COUNT(*) AS Order_Count
FROM
    Customer_Analysis
GROUP BY
    review_score
ORDER BY
    review_score
INTO OUTFILE '/Users/oseulgi/Desktop/[SQL] 포트폴리오용/order_count_by_review_score.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
