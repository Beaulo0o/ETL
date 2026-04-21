# Архитектура Retail Weather ETL

## Обзор системы

Retail Weather ETL Analytics - это data pipeline для сбора, обработки и анализа данных о розничных продажах с учетом погодных условий.

## C4 Диаграммы

### Level 1: System Context

```plantuml
@startuml
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

Person(data_analyst, "Data Analyst", "Анализирует данные о продажах")
Person(data_engineer, "Data Engineer", "Поддерживает ETL процессы")

System(retail_etl, "Retail Weather ETL", "Сбор, обработка и анализ данных о продажах и погоде")

System_Ext(weather_api, "OpenWeatherMap API", "Предоставляет данные о погоде")
System_Ext(sales_system, "Sales System", "Источник данных о продажах")

Rel(data_analyst, retail_etl, "Получает отчеты", "Excel/CSV")
Rel(data_engineer, retail_etl, "Управляет", "Airflow UI")
Rel(retail_etl, weather_api, "Запрашивает погоду", "HTTPS/REST")
Rel(retail_etl, sales_system, "Импортирует продажи", "CSV/API")

@enduml