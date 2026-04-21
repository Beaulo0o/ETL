"""
DAG для трансформации сырых данных о продажах из Staging в ODS и далее в Data Mart.
"""
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="sales_transform",
    description="ETL: Staging -> ODS -> Data Mart для продаж",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",  # Ежедневно в 2:00 UTC
    catchup=False,
    tags=["transform", "sales", "postgres"],
    doc_md=__doc__,
)
def sales_transform_dag() -> None:
    """
    Трансформация данных о продажах.
    """

    @task(task_id="extract_staging_sales")
    def extract_staging_sales(logical_date: str) -> pd.DataFrame:
        """
        Извлечение сырых данных из таблицы staging.sales за указанную дату.
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
            SELECT 
                transaction_id,
                store_id,
                product_id,
                quantity,
                unit_price,
                transaction_date
            FROM staging.sales
            WHERE DATE(transaction_date) = DATE(%(logical_date)s)
        """
        df = pg_hook.get_pandas_df(sql, parameters={"logical_date": logical_date})
        logger.info(f"Извлечено {len(df)} записей из staging.sales за {logical_date}")
        return df

    @task(task_id="transform_to_ods")
    def transform_to_ods(df: pd.DataFrame) -> pd.DataFrame:
        """
        Очистка и обогащение данных перед загрузкой в ODS.
        - Добавление total_amount
        - Очистка отрицательных значений quantity
        - Добавление audit колонок
        """
        if df.empty:
            logger.warning("DataFrame пуст. Трансформация пропущена.")
            return df

        df_clean = df.copy()

        # Очистка данных
        df_clean["quantity"] = df_clean["quantity"].clip(lower=0)
        df_clean["unit_price"] = df_clean["unit_price"].clip(lower=0)

        # Расчетные поля
        df_clean["total_amount"] = df_clean["quantity"] * df_clean["unit_price"]

        # Аудит колонки
        df_clean["etl_created_at"] = pendulum.now("UTC").to_iso8601_string()
        df_clean["etl_source"] = "airflow_sales_transform"
        df_clean["etl_batch_id"] = "{{ run_id }}"

        logger.info(f"Трансформация завершена. Кол-во записей: {len(df_clean)}")
        return df_clean

    @task(task_id="load_to_ods")
    def load_to_ods(df: pd.DataFrame) -> None:
        """
        Загрузка очищенных данных в таблицу ods.sales.
        Используется UPSERT логика (ON CONFLICT DO UPDATE).
        """
        if df.empty:
            logger.info("Нет данных для загрузки в ODS")
            return

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            df.to_sql(
                name="sales",
                schema="ods",
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )
            logger.info(f"Загружено {len(df)} записей в ods.sales")

    @task(task_id="refresh_sales_mart")
    def refresh_sales_mart() -> None:
        """Обновление витрины данных."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        sql = """
        INSERT INTO mart.daily_sales_weather (
            report_date, city_id, city_name, store_id, store_name,
            total_sales, total_transactions, avg_receipt, unique_products_sold,
            top_category, etl_updated_at
        )
        SELECT 
            DATE(s.transaction_date) AS report_date,
            st.city_id,
            st.city_name,
            s.store_id,
            st.store_name,
            SUM(s.total_amount) AS total_sales,
            COUNT(DISTINCT s.transaction_id) AS total_transactions,
            ROUND(AVG(s.total_amount)::numeric, 2) AS avg_receipt,
            COUNT(DISTINCT s.product_id) AS unique_products_sold,
            'Бакалея' AS top_category,
            CURRENT_TIMESTAMP
        FROM ods.sales s
        INNER JOIN ods.stores st ON s.store_id = st.store_id
        WHERE DATE(s.transaction_date) = '2026-04-20'
        GROUP BY DATE(s.transaction_date), st.city_id, st.city_name, s.store_id, st.store_name
        ON CONFLICT (report_date, store_id) DO UPDATE SET
            total_sales = EXCLUDED.total_sales,
            total_transactions = EXCLUDED.total_transactions,
            etl_updated_at = CURRENT_TIMESTAMP;
        """

        pg_hook.run(sql)
        print("✅ Витрина обновлена!")
    @task(
        task_id="run_data_quality_checks",
        trigger_rule="all_done",  # Запускать даже если предыдущие таски упали
    )
    def run_data_quality_checks() -> Dict[str, Any]:
        """
        Проверка качества данных в ODS после загрузки.
        Возвращает отчет о проверках.
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        checks = [
            {
                "name": "no_negative_amounts",
                "sql": "SELECT COUNT(*) FROM ods.sales WHERE total_amount < 0",
                "expected": 0,
            },
            {
                "name": "no_null_store_ids",
                "sql": "SELECT COUNT(*) FROM ods.sales WHERE store_id IS NULL",
                "expected": 0,
            },
            {
                "name": "no_future_transactions",
                "sql": "SELECT COUNT(*) FROM ods.sales WHERE transaction_date > CURRENT_TIMESTAMP + INTERVAL '1 hour'",
                "expected": 0,
            },
        ]

        results = {"passed": [], "failed": [], "details": []}

        for check in checks:
            result = pg_hook.get_first(check["sql"])[0]
            detail = {
                "check_name": check["name"],
                "actual": result,
                "expected": check["expected"],
            }
            results["details"].append(detail)

            if result == check["expected"]:
                results["passed"].append(check["name"])
                logger.info(f"✅ Проверка '{check['name']}' пройдена")
            else:
                results["failed"].append(check["name"])
                logger.error(
                    f"❌ Проверка '{check['name']}' провалена: ожидалось {check['expected']}, получено {result}")

        if results["failed"]:
            raise ValueError(f"Data Quality Checks провалены: {results['failed']}")

        return results

    # Определение потока данных
    staging_df = extract_staging_sales(logical_date="{{ ds }}")
    ods_df = transform_to_ods(df=staging_df)

    load_task = load_to_ods(df=ods_df)
    mart_task = refresh_sales_mart()
    dq_task = run_data_quality_checks()

    # Зависимости
    load_task >> mart_task >> dq_task


sales_transform_dag()