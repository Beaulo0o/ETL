-- =====================================================
-- Дополнительные индексы для оптимизации запросов
-- Выполняется после начальной загрузки данных
-- =====================================================

-- Составные индексы для частых JOIN запросов
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ods_sales_store_date
ON ods.sales(store_id, transaction_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ods_sales_product_date
ON ods.sales(product_id, transaction_date);

-- Индексы для аналитических запросов по витрине
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mart_dsw_city_date
ON mart.daily_sales_weather(city_name, report_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mart_dsw_weather_temp
ON mart.daily_sales_weather(weather_condition, avg_temperature_c);

-- Частичные индексы для активных записей
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ods