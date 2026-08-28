# Supermarket ETL Pipeline

ETL pipeline xử lý dữ liệu siêu thị sử dụng Apache Airflow + PostgreSQL trên Docker.

## Kiến trúc

```
Supermarket_sales.xlsx
        ↓
  [Bronze] bronze.supermarket        ← load raw data từ Excel
        ↓
  [Silver] silver.supermarket        ← làm sạch, validate, dedup
        ↓
  [Gold]   gold.supermarket_daily_summary   ← tổng hợp theo ngày/chi nhánh/sản phẩm
```

**DAG flow (7 tasks):**
```
start → load_to_bronze → clean_to_silver → log_rejection_rate → build_gold → validate_gold → end
```

## Luồng xử lý DAG

| Task | Operator | Mô tả |
|---|---|---|
| `start` | EmptyOperator | Đánh dấu bắt đầu pipeline (không xử lý) |
| `load_to_bronze` | PythonOperator | Đọc `Supermarket_sales.xlsx`, chuẩn hóa tên cột, nạp toàn bộ vào `bronze.supermarket`; tạo schemas và bảng `etl.etl_log` nếu chưa có |
| `clean_to_silver` | PostgresOperator | Chạy `silver_supermarket.sql`: loại duplicate theo `invoice_id` (DISTINCT ON), ép kiểu date/time, lọc 7 điều kiện dữ liệu bẩn → lưu vào `silver.supermarket` |
| `log_rejection_rate` | PythonOperator | So sánh số dòng Bronze vs Silver, tính tỷ lệ dữ liệu bị loại (%), ghi vào `etl.etl_log` |
| `build_gold` | PostgresOperator | Chạy `gold_supermarket.sql`: GROUP BY 6 chiều (ngày, chi nhánh, sản phẩm, thanh toán...), UPSERT vào `gold.supermarket_daily_summary` |
| `validate_gold` | PythonOperator | Kiểm tra 5 điều kiện: bảng không rỗng, không doanh thu âm, không branch NULL, avg_rating trong 1-10, total_quantity > 0 |
| `end` | EmptyOperator | Đánh dấu kết thúc pipeline (không xử lý) |

## Công nghệ sử dụng

- **Apache Airflow 2.8.1** — orchestration, lên lịch và quản lý pipeline
- **PostgreSQL 15** — lưu trữ dữ liệu ETL (Bronze/Silver/Gold)
- **Docker + Docker Compose** — containerize toàn bộ hệ thống
- **pandas / openpyxl** — đọc file Excel vào Bronze
- **SQLAlchemy** — kết nối Python với PostgreSQL

## Cấu trúc thư mục

```
Project/
├── data/
│   └── Supermarket_sales.xlsx     ← dataset nguồn
├── dags/
│   └── supermarket_etl_pipeline.py  ← DAG chính
├── sql/
│   ├── silver_supermarket.sql     ← làm sạch Bronze → Silver
│   └── gold_supermarket.sql       ← tổng hợp Silver → Gold
├── init/
│   └── 01_create_etl_db.sql       ← tạo DB và user khi khởi động lần đầu
├── logs/
├── plugins/
└── docker-compose.yaml
```

## Yêu cầu hệ thống

- **Docker Desktop** >= 20.10
- **Docker Compose** (tích hợp sẵn trong Docker Desktop)
- Port **8080** còn trống (Airflow UI)
- Port **5432** còn trống (PostgreSQL)

Kiểm tra bằng lệnh:
```bash
docker --version
docker compose version
```

## Cách chạy

```bash
# Lần đầu — khởi tạo DB và tạo user admin
docker compose up airflow-init

# Chạy toàn bộ hệ thống
docker compose up -d
```

Mở trình duyệt: **http://localhost:8080**
- Username: `admin`
- Password: `admin`

Vào DAG `supermarket_etl_pipeline` → nhấn **Trigger DAG ▶**

## Query kết quả

```sql
-- Doanh thu theo chi nhánh
SELECT branch, city, SUM(total_revenue) AS revenue
FROM gold.supermarket_daily_summary
GROUP BY branch, city
ORDER BY revenue DESC;

-- Product line bán chạy nhất
SELECT product_line,
       SUM(total_transactions) AS so_giao_dich,
       ROUND(AVG(avg_rating)::NUMERIC, 2) AS diem_danh_gia
FROM gold.supermarket_daily_summary
GROUP BY product_line
ORDER BY so_giao_dich DESC;

-- Phương thức thanh toán theo chi nhánh
SELECT branch, payment, SUM(total_transactions) AS so_lan
FROM gold.supermarket_daily_summary
GROUP BY branch, payment
ORDER BY branch, so_lan DESC;
```
