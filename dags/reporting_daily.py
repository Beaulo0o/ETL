
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pandas as pd
import io
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="reporting_daily",
    description="Генерация ежедневного отчета по продажам и погоде",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 7 * * *",  # Ежедневно в 7:00 UTC
    catchup=False,
    tags=["reporting", "export"],
    doc_md=__doc__,
)
def reporting_daily_dag() -> None:
    """
    Формирование и экспорт отчета.
    """

    @task(task_id="generate_sales_report")
    def generate_sales_report() -> pd.DataFrame:
        """
        Генерация сводного отчета из витрины данных.
        """
        import pendulum
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        logical_date = pendulum.today().to_date_string()

        sql = """
            SELECT 
                report_date,
                city_name,
                store_name,
                total_sales,
                total_transactions,
                avg_receipt,
                avg_temperature_c,
                weather_condition
            FROM mart.daily_sales_weather
            WHERE report_date = DATE(%(logical_date)s)
            ORDER BY city_name, store_name
        """

        df = pg_hook.get_pandas_df(sql, parameters={"logical_date": logical_date})
        print(f"Сгенерирован отчет для даты {logical_date}: {len(df)} строк")
        return df

    @task(task_id="export_to_excel_s3")
    def export_to_excel_s3(df: pd.DataFrame, logical_date: str) -> Dict[str, str]:
        """
        Экспорт DataFrame в Excel и сохранение в S3.
        Возвращает словарь с путями к файлам.
        """
        if df.empty:
            logger.warning("Нет данных для экспорта")
            return {"status": "no_data", "paths": []}

        s3_hook = S3Hook(aws_conn_id="minio_default")
        bucket_name = Variable.get("REPORTS_BUCKET", "reports")

        # Создание Excel файла в памяти
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Sales_Weather", index=False)

            # Форматирование
            workbook = writer.book
            worksheet = writer.sheets["Sales_Weather"]

            # Формат для денежных значений
            money_format = workbook.add_format({"num_format": "#,##0.00 ₽"})
            worksheet.set_column("D:D", 15, money_format)  # total_sales
            worksheet.set_column("H:H", 15, money_format)  # avg_receipt

            # Автофильтр
            worksheet.autofilter(0, 0, 0, len(df.columns) - 1)

        output.seek(0)

        # Партиционирование в S3
        date_obj = pendulum.parse(logical_date)
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")

        key = f"daily_reports/year={year}/month={month}/day={day}/sales_weather_report_{logical_date}.xlsx"

        s3_hook.load_bytes(
            bytes_data=output.getvalue(),
            key=key,
            bucket_name=bucket_name,
            replace=True,
        )

        file_url = f"s3://{bucket_name}/{key}"
        logger.info(f"Отчет сохранен: {file_url}")

        return {
            "status": "success",
            "file_url": file_url,
            "row_count": len(df),
            "date": logical_date,
        }

    @task(task_id="send_notification")
    def send_notification(export_result: Dict[str, Any]) -> None:
        """
        Отправка уведомления о готовности отчета (эмуляция).
        """
        if export_result.get("status") == "success":
            message = f"""
            ✅ Ежедневный отчет готов!

            📅 Дата: {export_result.get('date')}
            📊 Количество строк: {export_result.get('row_count')}
            📁 Файл: {export_result.get('file_url')}

            Отчет доступен для скачивания из S3.
            """
            logger.info(message)
        else:
            logger.warning(f"Отчет не сформирован: {export_result}")

    # Поток задач
    report_df = generate_sales_report()
    export_info = export_to_excel_s3(df=report_df, logical_date="{{ ds }}")
    send_notification(export_result=export_info)


reporting_daily_dag()