"""
DAG для извлечения данных о погоде из OpenWeatherMap API и сохранения в Data Lake (MinIO).
"""

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-eng@company.com"],
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

CITIES = ["Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg", "Kazan"]


@dag(
    dag_id="weather_ingestion",
    description="Извлечение текущей погоды из OpenWeather API -> Raw S3 (MinIO)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    tags=["ingestion", "weather", "api"],
    doc_md=__doc__,
    max_active_runs=1,
)
def weather_ingestion_dag() -> None:
    """
    Основная функция, определяющая граф задач DAG.
    """

    @task(
        task_id="extract_weather_data",
        retries=3,
        retry_delay=pendulum.duration(seconds=30),
    )
    def extract_weather_data(cities: List[str], logical_date: str) -> List[Dict[str, Any]]:
        """
        Извлекает данные о погоде для списка городов через OpenWeatherMap API.
        """
        api_key = Variable.get("OPENWEATHER_API_KEY")
        if not api_key:
            raise ValueError("API ключ не найден в Airflow Variables")

        results = []
        for city in cities:
            try:
                url = "http://api.openweathermap.org/data/2.5/weather"
                params = {
                    "q": city,
                    "appid": api_key,
                    "units": "metric",
                    "lang": "ru",
                }
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()
                data["_metadata"] = {
                    "extraction_timestamp": pendulum.now("UTC").isoformat(),
                    "logical_date": logical_date,
                    "city_requested": city,
                }
                results.append(data)
                logger.info(f"Успешно получены данные для {city}")

            except Exception as e:
                logger.error(f"Ошибка при получении данных для {city}: {e}")
                raise

        return results

    @task(task_id="save_raw_to_s3")
    def save_raw_to_s3(weather_data: List[Dict[str, Any]]) -> None:
        """
        Сохраняет сырые JSON данные в S3 (MinIO) в партиционированную структуру.
        """
        s3_hook = S3Hook(aws_conn_id="minio_default")
        bucket_name = Variable.get("RAW_DATA_BUCKET", "raw-data")

        execution_date = pendulum.now("UTC")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")
        hour = execution_date.strftime("%H")

        for record in weather_data:
            city = record.get("name", "unknown")
            timestamp = pendulum.now("UTC").timestamp()

            key = f"weather/year={year}/month={month}/day={day}/hour={hour}/{city}_{timestamp}.json"

            s3_hook.load_string(
                string_data=json.dumps(record, ensure_ascii=False, indent=2),
                key=key,
                bucket_name=bucket_name,
                replace=True,
            )
            logger.info(f"Файл сохранен в s3://{bucket_name}/{key}")

        logger.info(f"Всего сохранено {len(weather_data)} файлов")

    @task(task_id="load_weather_to_postgres")
    def load_weather_to_postgres(weather_data: List[Dict[str, Any]]) -> None:
        """Загружает данные о погоде в staging.weather_raw."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import json

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        for record in weather_data:
            city = record.get("name")
            extraction_time = record.get("_metadata", {}).get("extraction_timestamp")

            sql = """
            INSERT INTO staging.weather_raw (city_name, raw_json, extraction_timestamp)
            VALUES (%s, %s, %s)
            """

            pg_hook.run(sql, parameters=(
                city,
                json.dumps(record, ensure_ascii=False),
                extraction_time
            ))

        print(f"✅ Загружено {len(weather_data)} записей в staging.weather_raw")
    raw_data = extract_weather_data(cities=CITIES, logical_date="{{ ds }}")
    save_raw_to_s3(weather_data=raw_data)
    load_weather_to_postgres(weather_data=raw_data) 
weather_ingestion_dag()
