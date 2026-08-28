from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import uuid
import pandas as pd
from sqlalchemy import create_engine, text

default_args = {
    'owner': 'etl',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_PATH = '/opt/airflow/data/Supermarket_sales.xlsx'
CONN_STR = 'postgresql+psycopg2://etl_user:etl_pass@postgres:5432/supermarket_etl'


def _log(conn, run_id, step, status, msg=""):
    """Ghi log vào etl.etl_log"""
    conn.execute(
        text("INSERT INTO etl.etl_log(run_id, step_name, status, message) VALUES (:r,:s,:st,:m)"),
        {"r": run_id, "s": step, "st": status, "m": msg}
    )


def load_bronze():
    run_id = f"RUN_{uuid.uuid4().hex[:8]}"
    engine = create_engine(CONN_STR)

    with engine.begin() as conn:
        # Tạo schemas + bảng log (idempotent — chạy bao nhiêu lần cũng OK)
        for stmt in [
            'CREATE SCHEMA IF NOT EXISTS bronze',
            'CREATE SCHEMA IF NOT EXISTS silver',
            'CREATE SCHEMA IF NOT EXISTS gold',
            'CREATE SCHEMA IF NOT EXISTS etl',
            '''CREATE TABLE IF NOT EXISTS etl.etl_log (
                log_id     BIGSERIAL PRIMARY KEY,
                run_id     TEXT NOT NULL,
                step_name  TEXT NOT NULL,
                status     TEXT NOT NULL,
                message    TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
        ]:
            conn.execute(text(stmt))
        _log(conn, run_id, "INGEST", "START", f"Reading {DATA_PATH}")

    df = pd.read_excel(DATA_PATH)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_', regex=False)
        .str.replace('%', 'pct', regex=False)
    )
    df.to_sql('supermarket', engine, schema='bronze', if_exists='replace', index=False)

    with engine.begin() as conn:
        _log(conn, run_id, "INGEST", "SUCCESS", f"{len(df)} rows → bronze.supermarket")
    print(f"Bronze loaded: {len(df)} rows | run_id={run_id}")


def validate_gold():
    run_id = f"RUN_{uuid.uuid4().hex[:8]}"
    engine = create_engine(CONN_STR)

    with engine.begin() as conn:
        _log(conn, run_id, "VALIDATE", "START", "Checking gold table")

    with engine.connect() as conn:
        count = conn.execute(
            text('SELECT COUNT(*) FROM gold.supermarket_daily_summary')
        ).scalar()
        assert count > 0, "Gold table rỗng!"

        neg = conn.execute(
            text('SELECT COUNT(*) FROM gold.supermarket_daily_summary WHERE total_revenue < 0')
        ).scalar()
        assert neg == 0, f"Có {neg} dòng total_revenue âm!"

        null_branch = conn.execute(
            text('SELECT COUNT(*) FROM gold.supermarket_daily_summary WHERE branch IS NULL')
        ).scalar()
        assert null_branch == 0, "Có dòng branch NULL trong gold!"

        bad_rating = conn.execute(
            text('SELECT COUNT(*) FROM gold.supermarket_daily_summary WHERE avg_rating NOT BETWEEN 1 AND 10')
        ).scalar()
        assert bad_rating == 0, f"Có {bad_rating} dòng avg_rating ngoài khoảng 1-10!"

        neg_qty = conn.execute(
            text('SELECT COUNT(*) FROM gold.supermarket_daily_summary WHERE total_quantity <= 0')
        ).scalar()
        assert neg_qty == 0, f"Có {neg_qty} dòng total_quantity <= 0!"

    with engine.begin() as conn:
        _log(conn, run_id, "VALIDATE", "SUCCESS", f"{count} rows in gold, all 5 checks passed")
    print(f"validate_gold PASSED — {count} dòng | run_id={run_id}")


def log_rejection_rate():
    """So sánh số dòng Bronze vs Silver, ghi rejection rate vào etl_log"""
    run_id = f"RUN_{uuid.uuid4().hex[:8]}"
    engine = create_engine(CONN_STR)

    with engine.connect() as conn:
        bronze = conn.execute(text('SELECT COUNT(*) FROM bronze.supermarket')).scalar()
        silver = conn.execute(text('SELECT COUNT(*) FROM silver.supermarket')).scalar()
        rejected = bronze - silver
        rate = round(rejected / bronze * 100, 2) if bronze > 0 else 0

    with engine.begin() as conn:
        _log(conn, run_id, "REJECTION_RATE", "INFO",
             f"Bronze={bronze}, Silver={silver}, Rejected={rejected} ({rate}%)")
    print(f"Rejection rate: {rate}% ({rejected}/{bronze} dòng bị loại) | run_id={run_id}")


with DAG(
    dag_id='supermarket_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    load_to_bronze = PythonOperator(
        task_id='load_to_bronze',
        python_callable=load_bronze,
    )

    clean_to_silver = PostgresOperator(
        task_id='clean_to_silver',
        postgres_conn_id='postgres_etl',
        sql='sql/silver_supermarket.sql',
    )

    build_gold = PostgresOperator(
        task_id='build_gold',
        postgres_conn_id='postgres_etl',
        sql='sql/gold_supermarket.sql',
    )

    log_rejection = PythonOperator(
        task_id='log_rejection_rate',
        python_callable=log_rejection_rate,
    )

    validate = PythonOperator(
        task_id='validate_gold',
        python_callable=validate_gold,
    )

    start >> load_to_bronze >> clean_to_silver >> log_rejection >> build_gold >> validate >> end
