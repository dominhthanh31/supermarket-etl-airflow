-- Tạo bảng nếu chưa có (không DROP — idempotent)
CREATE TABLE IF NOT EXISTS gold.supermarket_daily_summary (
    report_date         DATE,
    branch              TEXT,
    city                TEXT,
    product_line        TEXT,
    payment             TEXT,
    customer_type       TEXT,
    total_transactions  BIGINT,
    total_quantity      BIGINT,
    total_revenue       NUMERIC(12,2),
    total_tax           NUMERIC(12,2),
    total_gross_income  NUMERIC(12,2),
    avg_unit_price      NUMERIC(10,2),
    avg_rating          NUMERIC(4,2),
    created_at          TIMESTAMP,
    PRIMARY KEY (report_date, branch, product_line, payment, customer_type)
);

-- UPSERT: nếu trigger lại DAG, cập nhật số liệu thay vì báo lỗi primary key trùng
INSERT INTO gold.supermarket_daily_summary
SELECT
    sale_date                               AS report_date,
    branch,
    city,
    product_line,
    payment,
    customer_type,
    COUNT(*)                                AS total_transactions,
    SUM(quantity)                           AS total_quantity,
    ROUND(SUM(total)::NUMERIC, 2)           AS total_revenue,
    ROUND(SUM(tax_5pct)::NUMERIC, 2)        AS total_tax,
    ROUND(SUM(gross_income)::NUMERIC, 2)    AS total_gross_income,
    ROUND(AVG(unit_price)::NUMERIC, 2)      AS avg_unit_price,
    ROUND(AVG(rating)::NUMERIC, 2)          AS avg_rating,
    NOW()                                   AS created_at
FROM silver.supermarket
GROUP BY
    sale_date, branch, city, product_line, payment, customer_type
ON CONFLICT (report_date, branch, product_line, payment, customer_type)
DO UPDATE SET
    city               = EXCLUDED.city,
    total_transactions = EXCLUDED.total_transactions,
    total_quantity     = EXCLUDED.total_quantity,
    total_revenue      = EXCLUDED.total_revenue,
    total_tax          = EXCLUDED.total_tax,
    total_gross_income = EXCLUDED.total_gross_income,
    avg_unit_price     = EXCLUDED.avg_unit_price,
    avg_rating         = EXCLUDED.avg_rating,
    created_at         = EXCLUDED.created_at;
