# Retail Weather ETL Analytics

##  Описание проекта

**Retail Weather ETL Analytics** - это полноценный ETL пайплайн для анализа влияния погоды на розничные продажи. Проект демонстрирует лучшие практики Data Engineering:

-  Оркестрация ETL процессов с Apache Airflow
-  Трансформация данных с Python и SQL
-  Работа с PostgreSQL (Staging, ODS, Data Mart)
-  Хранение сырых данных в S3-совместимом хранилище (MinIO)
-  Мониторинг с Prometheus + Grafana
-  Полное тестирование (unit, integration, data quality)
-  Полная контейнеризация с Docker Compose
-  CI/CD пайплайн с GitHub Actions

##  Ключевые особенности

### Архитектура данных
```mermaid
graph LR
    A[OpenWeather API] --> B[Airflow DAG]
    C[Sales Data Source] --> B
    B --> D[MinIO S3 Raw Layer]
    B --> E[PostgreSQL Staging]
    E --> F[PostgreSQL ODS]
    F --> G[PostgreSQL Data Mart]
    G --> H[Excel Reports]
    B --> I[Data Quality Checks]