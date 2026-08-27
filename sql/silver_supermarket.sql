DROP TABLE IF EXISTS silver.supermarket;

CREATE TABLE silver.supermarket AS
WITH dedup AS (
    -- DISTINCT ON: nếu invoice_id bị trùng, chỉ giữ 1 dòng 
    SELECT DISTINCT ON (invoice_id) *
    FROM bronze.supermarket
    ORDER BY invoice_id
)
SELECT
    invoice_id,
    branch,
    city,
    customer_type,
    gender,
    product_line,
    unit_price,
    quantity,
    tax_5pct,
    total,
    date::DATE                  AS sale_date,
    time::TIME                  AS sale_time,
    payment,
    cogs,
    gross_margin_percentage,
    gross_income,
    rating
FROM dedup
WHERE
    invoice_id IS NOT NULL
    AND branch IN ('A', 'B', 'C')
    AND unit_price > 0
    AND quantity BETWEEN 1 AND 10
    AND rating BETWEEN 1.0 AND 10.0
    AND ABS(total - unit_price * quantity * 1.05) < 0.01
    AND total > 0;
