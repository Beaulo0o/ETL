-- =====================================================
-- DDL скрипт для создания всех таблиц в проекте
-- Слои: staging, ods, mart
-- =====================================================

-- =====================================================
-- STAGING LAYER (сырые данные)
-- =====================================================

-- Создание схемы staging
CREATE SCHEMA IF NOT EXISTS staging;

-- Таблица сырых данных о продажах
DROP TABLE IF EXISTS staging.sales CASCADE;
CREATE TABLE staging.sales (
    transaction_id VARCHAR(50) NOT NULL,
    store_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    -- Аудит колонки
    _imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file VARCHAR(500),
    _batch_id VARCHAR(100)
);

COMMENT ON TABLE staging.sales IS 'Сырые данные о продажах из источников';
COMMENT ON COLUMN staging.sales.transaction_id IS 'Уникальный идентификатор транзакции';
COMMENT ON COLUMN staging.sales.store_id IS 'ID магазина';
COMMENT ON COLUMN staging.sales.product_id IS 'ID товара';
COMMENT ON COLUMN staging.sales.quantity IS 'Количество единиц товара';
COMMENT ON COLUMN staging.sales.unit_price IS 'Цена за единицу товара';
COMMENT ON COLUMN staging.sales.transaction_date IS 'Дата и время транзакции';
COMMENT ON COLUMN staging.sales._imported_at IS 'Техническое поле: timestamp импорта';
COMMENT ON COLUMN staging.sales._source_file IS 'Техническое поле: источник данных';
COMMENT ON COLUMN staging.sales._batch_id IS 'Техническое поле: ID батча загрузки';

-- Индексы для staging
CREATE INDEX idx_staging_sales_transaction_date ON staging.sales(transaction_date);
CREATE INDEX idx_staging_sales_transaction_id ON staging.sales(transaction_id);
CREATE INDEX idx_staging_sales_store_id ON staging.sales(store_id);
CREATE INDEX idx_staging_sales_product_id ON staging.sales(product_id);

-- =====================================================
-- ODS LAYER (Operational Data Store - очищенные данные)
-- =====================================================

-- Создание схемы ods
CREATE SCHEMA IF NOT EXISTS ods;

-- Таблица очищенных данных о продажах
DROP TABLE IF EXISTS ods.sales CASCADE;
CREATE TABLE ods.sales (
    -- Бизнес ключи
    transaction_id VARCHAR(50) NOT NULL,
    store_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,

    -- Метрики
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount DECIMAL(12, 2) NOT NULL CHECK (total_amount >= 0),

    -- Временные метки
    transaction_date TIMESTAMP NOT NULL,

    -- Аудит колонки
    etl_created_at TIMESTAMP NOT NULL,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_source VARCHAR(100) NOT NULL,
    etl_batch_id VARCHAR(100) NOT NULL,

    -- Первичный ключ
    CONSTRAINT pk_ods_sales PRIMARY KEY (transaction_id, store_id, product_id)
);

COMMENT ON TABLE ods.sales IS 'Очищенные и валидированные данные о продажах (ODS слой)';
COMMENT ON COLUMN ods.sales.transaction_id IS 'Уникальный идентификатор транзакции';
COMMENT ON COLUMN ods.sales.store_id IS 'ID магазина (FK к справочнику stores)';
COMMENT ON COLUMN ods.sales.product_id IS 'ID товара (FK к справочнику products)';
COMMENT ON COLUMN ods.sales.quantity IS 'Количество единиц товара (>= 0)';
COMMENT ON COLUMN ods.sales.unit_price IS 'Цена за единицу товара (>= 0)';
COMMENT ON COLUMN ods.sales.total_amount IS 'Общая сумма позиции (quantity * unit_price)';
COMMENT ON COLUMN ods.sales.transaction_date IS 'Дата и время транзакции';
COMMENT ON COLUMN ods.sales.etl_created_at IS 'Дата и время создания записи в ODS';
COMMENT ON COLUMN ods.sales.etl_updated_at IS 'Дата и время последнего обновления записи';
COMMENT ON COLUMN ods.sales.etl_source IS 'Источник данных (название DAG/процесса)';
COMMENT ON COLUMN ods.sales.etl_batch_id IS 'Идентификатор батча загрузки';

-- Индексы для ODS
CREATE INDEX idx_ods_sales_transaction_date ON ods.sales(transaction_date);
CREATE INDEX idx_ods_sales_store_id ON ods.sales(store_id);
CREATE INDEX idx_ods_sales_product_id ON ods.sales(product_id);
CREATE INDEX idx_ods_sales_etl_created_at ON ods.sales(etl_created_at);

-- Справочник магазинов
DROP TABLE IF EXISTS ods.stores CASCADE;
CREATE TABLE ods.stores (
    store_id INTEGER PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    city_id INTEGER NOT NULL,
    city_name VARCHAR(50) NOT NULL,
    store_type VARCHAR(50) CHECK (store_type IN ('supermarket', 'hypermarket', 'convenience', 'discount')),
    opening_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    etl_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE ods.stores IS 'Справочник магазинов';
COMMENT ON COLUMN ods.stores.store_id IS 'Уникальный идентификатор магазина';
COMMENT ON COLUMN ods.stores.store_name IS 'Название магазина';
COMMENT ON COLUMN ods.stores.city_id IS 'ID города';
COMMENT ON COLUMN ods.stores.city_name IS 'Название города';
COMMENT ON COLUMN ods.stores.store_type IS 'Тип магазина';
COMMENT ON COLUMN ods.stores.opening_date IS 'Дата открытия магазина';
COMMENT ON COLUMN ods.stores.is_active IS 'Флаг активности магазина';

-- Вставка тестовых данных в справочник магазинов
INSERT INTO ods.stores (store_id, store_name, city_id, city_name, store_type, opening_date, is_active) VALUES
(1, 'Центральный', 1, 'Moscow', 'supermarket', '2020-01-15', TRUE),
(2, 'Северный', 2, 'Saint Petersburg', 'hypermarket', '2019-06-20', TRUE),
(3, 'Южный', 3, 'Novosibirsk', 'convenience', '2021-03-10', TRUE),
(4, 'Западный', 4, 'Yekaterinburg', 'supermarket', '2020-11-05', TRUE),
(5, 'Восточный', 5, 'Kazan', 'supermarket', '2021-08-12', TRUE),
(6, 'Приморский', 1, 'Moscow', 'convenience', '2022-02-28', TRUE),
(7, 'Горный', 4, 'Yekaterinburg', 'hypermarket', '2018-12-01', TRUE)
ON CONFLICT (store_id) DO UPDATE SET
    store_name = EXCLUDED.store_name,
    city_name = EXCLUDED.city_name,
    store_type = EXCLUDED.store_type,
    etl_updated_at = CURRENT_TIMESTAMP;

-- Справочник товаров
DROP TABLE IF EXISTS ods.products CASCADE;
CREATE TABLE ods.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    base_price DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    etl_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE ods.products IS 'Справочник товаров';
COMMENT ON COLUMN ods.products.product_id IS 'Уникальный идентификатор товара';
COMMENT ON COLUMN ods.products.product_name IS 'Название товара';
COMMENT ON COLUMN ods.products.category IS 'Категория товара';
COMMENT ON COLUMN ods.products.subcategory IS 'Подкатегория товара';
COMMENT ON COLUMN ods.products.base_price IS 'Базовая цена товара';
COMMENT ON COLUMN ods.products.is_active IS 'Флаг активности товара';

-- Вставка тестовых данных в справочник товаров
INSERT INTO ods.products (product_id, product_name, category, subcategory, base_price, is_active) VALUES
(101, 'Хлеб', 'Бакалея', 'Хлебобулочные изделия', 45.00, TRUE),
(102, 'Молоко', 'Молочные продукты', 'Молоко и сливки', 89.00, TRUE),
(103, 'Яйца (10 шт)', 'Молочные продукты', 'Яйца', 120.00, TRUE),
(104, 'Курица', 'Мясо', 'Птица', 250.00, TRUE),
(105, 'Говядина', 'Мясо', 'Красное мясо', 550.00, TRUE),
(106, 'Яблоки', 'Фрукты', 'Сезонные фрукты', 120.00, TRUE),
(107, 'Бананы', 'Фрукты', 'Экзотические фрукты', 90.00, TRUE),
(108, 'Вода 1.5л', 'Напитки', 'Вода', 55.00, TRUE),
(109, 'Сок апельсиновый', 'Напитки', 'Соки', 150.00, TRUE),
(110, 'Шоколад', 'Кондитерские изделия', 'Шоколад', 85.00, TRUE)
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    base_price = EXCLUDED.base_price,
    etl_updated_at = CURRENT_TIMESTAMP;

-- =====================================================
-- DATA MART LAYER (витрины данных)
-- =====================================================

-- Создание схемы mart
CREATE SCHEMA IF NOT EXISTS mart;

-- Витрина: дневная агрегация продаж с погодой
DROP TABLE IF EXISTS mart.daily_sales_weather CASCADE;
CREATE TABLE mart.daily_sales_weather (
    report_date DATE NOT NULL,
    city_id INTEGER NOT NULL,
    city_name VARCHAR(50) NOT NULL,
    store_id INTEGER NOT NULL,
    store_name VARCHAR(100) NOT NULL,

    -- Метрики продаж
    total_sales DECIMAL(14, 2) NOT NULL,
    total_transactions INTEGER NOT NULL,
    avg_receipt DECIMAL(10, 2),
    unique_products_sold INTEGER,
    top_category VARCHAR(100),

    -- Метрики погоды
    avg_temperature_c DECIMAL(5, 2),
    min_temperature_c DECIMAL(5, 2),
    max_temperature_c DECIMAL(5, 2),
    avg_humidity INTEGER,
    precipitation_mm DECIMAL(6, 2),
    weather_condition VARCHAR(50),

    -- Аудит
    etl_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, store_id)
);

COMMENT ON TABLE mart.daily_sales_weather IS 'Витрина: дневная агрегация продаж с погодными данными';
COMMENT ON COLUMN mart.daily_sales_weather.report_date IS 'Дата отчета';
COMMENT ON COLUMN mart.daily_sales_weather.city_id IS 'ID города';
COMMENT ON COLUMN mart.daily_sales_weather.city_name IS 'Название города';
COMMENT ON COLUMN mart.daily_sales_weather.store_id IS 'ID магазина';
COMMENT ON COLUMN mart.daily_sales_weather.store_name IS 'Название магазина';
COMMENT ON COLUMN mart.daily_sales_weather.total_sales IS 'Общая сумма продаж за день';
COMMENT ON COLUMN mart.daily_sales_weather.total_transactions IS 'Количество транзакций за день';
COMMENT ON COLUMN mart.daily_sales_weather.avg_receipt IS 'Средний чек';
COMMENT ON COLUMN mart.daily_sales_weather.unique_products_sold IS 'Количество уникальных проданных товаров';
COMMENT ON COLUMN mart.daily_sales_weather.top_category IS 'Самая продаваемая категория товаров';
COMMENT ON COLUMN mart.daily_sales_weather.avg_temperature_c IS 'Средняя температура за день (°C)';
COMMENT ON COLUMN mart.daily_sales_weather.min_temperature_c IS 'Минимальная температура за день (°C)';
COMMENT ON COLUMN mart.daily_sales_weather.max_temperature_c IS 'Максимальная температура за день (°C)';
COMMENT ON COLUMN mart.daily_sales_weather.avg_humidity IS 'Средняя влажность (%)';
COMMENT ON COLUMN mart.daily_sales_weather.precipitation_mm IS 'Количество осадков (мм)';
COMMENT ON COLUMN mart.daily_sales_weather.weather_condition IS 'Основное погодное условие';

-- Индексы для витрины
CREATE INDEX idx_mart_dsw_report_date ON mart.daily_sales_weather(report_date);
CREATE INDEX idx_mart_dsw_city_id ON mart.daily_sales_weather(city_id);
CREATE INDEX idx_mart_dsw_store_id ON mart.daily_sales_weather(store_id);

-- =====================================================
-- Сырые данные погоды (staging)
-- =====================================================

DROP TABLE IF EXISTS staging.weather_raw CASCADE;
CREATE TABLE staging.weather_raw (
    ingestion_id SERIAL,
    city_name VARCHAR(50) NOT NULL,
    raw_json JSONB NOT NULL,
    extracted_temperature DECIMAL(5, 2) GENERATED ALWAYS AS ((raw_json->'main'->>'temp')::DECIMAL(5,2)) STORED,
    extracted_humidity INTEGER GENERATED ALWAYS AS ((raw_json->'main'->>'humidity')::INTEGER) STORED,
    extracted_pressure INTEGER GENERATED ALWAYS AS ((raw_json->'main'->>'pressure')::INTEGER) STORED,
    weather_main VARCHAR(50) GENERATED ALWAYS AS (raw_json->'weather'->0->>'main') STORED,
    weather_description VARCHAR(100) GENERATED ALWAYS AS (raw_json->'weather'->0->>'description') STORED,
    wind_speed DECIMAL(5, 2) GENERATED ALWAYS AS ((raw_json->'wind'->>'speed')::DECIMAL(5,2)) STORED,
    extraction_timestamp TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ingestion_id)
);

COMMENT ON TABLE staging.weather_raw IS 'Сырые JSON данные погоды из API';
COMMENT ON COLUMN staging.weather_raw.raw_json IS 'Полный JSON ответ от API';
COMMENT ON COLUMN staging.weather_raw.extracted_temperature IS 'Извлеченная температура (генерируемая колонка)';
COMMENT ON COLUMN staging.weather_raw.extraction_timestamp IS 'Временная метка извлечения данных';

CREATE INDEX idx_weather_raw_city_timestamp ON staging.weather_raw(city_name, extraction_timestamp);
CREATE INDEX idx_weather_raw_raw_json ON staging.weather_raw USING GIN (raw_json);

-- =====================================================
-- ODS: Агрегированные погодные данные
-- =====================================================

DROP TABLE IF EXISTS ods.weather_daily CASCADE;
CREATE TABLE ods.weather_daily (
    date DATE NOT NULL,
    city_name VARCHAR(50) NOT NULL,
    avg_temperature DECIMAL(5, 2),
    min_temperature DECIMAL(5, 2),
    max_temperature DECIMAL(5, 2),
    avg_humidity INTEGER,
    avg_pressure INTEGER,
    total_precipitation DECIMAL(6, 2) DEFAULT 0,
    dominant_weather VARCHAR(50),
    measurements_count INTEGER,
    etl_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, city_name)
);

COMMENT ON TABLE ods.weather_daily IS 'Дневная агрегация погодных данных';
COMMENT ON COLUMN ods.weather_daily.date IS 'Дата наблюдений';
COMMENT ON COLUMN ods.weather_daily.city_name IS 'Название города';
COMMENT ON COLUMN ods.weather_daily.measurements_count IS 'Количество измерений за день';

-- =====================================================
-- Вспомогательные функции и процедуры
-- =====================================================

-- Функция для логирования ETL операций
CREATE OR REPLACE FUNCTION audit.etl_log(
    p_dag_id VARCHAR,
    p_task_id VARCHAR,
    p_execution_date TIMESTAMP,
    p_status VARCHAR,
    p_rows_affected INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    -- Создание схемы аудита если не существует
    CREATE SCHEMA IF NOT EXISTS audit;

    -- Создание таблицы аудита если не существует
    CREATE TABLE IF NOT EXISTS audit.etl_execution_log (
        log_id SERIAL PRIMARY KEY,
        dag_id VARCHAR(100) NOT NULL,
        task_id VARCHAR(100) NOT NULL,
        execution_date TIMESTAMP NOT NULL,
        status VARCHAR(20) NOT NULL CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED', 'WARNING')),
        rows_affected INTEGER,
        error_message TEXT,
        log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Вставка записи в лог
    INSERT INTO audit.etl_execution_log (
        dag_id, task_id, execution_date, status, rows_affected, error_message
    ) VALUES (
        p_dag_id, p_task_id, p_execution_date, p_status, p_rows_affected, p_error_message
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.etl_log IS 'Логирование выполнения ETL процессов';