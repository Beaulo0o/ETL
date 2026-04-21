-- =====================================================
-- Обновление витрины mart.daily_sales_weather
-- Используется в DAG sales_transform
-- Параметры: {{ ds }} - дата выполнения
-- =====================================================

WITH
-- 1. Агрегация продаж за день
daily_sales AS (
    SELECT
        DATE(s.transaction_date) AS report_date,
        st.city_id,
        st.city_name,
        s.store_id,
        st.store_name,
        COUNT(DISTINCT s.transaction_id) AS total_transactions,
        SUM(s.total_amount) AS total_sales,
        ROUND(AVG(s.total_amount), 2) AS avg_receipt,
        COUNT(DISTINCT s.product_id) AS unique_products_sold
    FROM ods.sales s
    INNER JOIN ods.stores st ON s.store_id = st.store_id
    WHERE DATE(s.transaction_date) = {{ ds }}
        AND st.is_active = TRUE
    GROUP BY
        DATE(s.transaction_date),
        st.city_id,
        st.city_name,
        s.store_id,
        st.store_name
),

-- 2. Определение самой продаваемой категории для каждого магазина
top_categories AS (
    SELECT DISTINCT ON (store_id)
        store_id,
        category AS top_category
    FROM (
        SELECT
            s.store_id,
            p.category,
            SUM(s.total_amount) AS category_sales,
            ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY SUM(s.total_amount) DESC) AS rn
        FROM ods.sales s
        INNER JOIN ods.products p ON s.product_id = p.product_id
        WHERE DATE(s.transaction_date) = {{ ds }}
        GROUP BY s.store_id, p.category
    ) ranked
    WHERE rn = 1
),

-- 3. Погодные данные за день
daily_weather AS (
    SELECT
        wd.date,
        wd.city_name,
        wd.avg_temperature,
        wd.min_temperature,
        wd.max_temperature,
        wd.avg_humidity,
        wd.total_precipitation,
        wd.dominant_weather
    FROM ods.weather_daily wd
    WHERE wd.date = {{ ds }}
)

-- 4. Формирование финальной витрины
INSERT INTO mart.daily_sales_weather (
    report_date,
    city_id,
    city_name,
    store_id,
    store_name,
    total_sales,
    total_transactions,
    avg_receipt,
    unique_products_sold,
    top_category,
    avg_temperature_c,
    min_temperature_c,
    max_temperature_c,
    avg_humidity,
    precipitation_mm,
    weather_condition,
    etl_updated_at
)
SELECT
    ds.report_date,
    ds.city_id,
    ds.city_name,
    ds.store_id,
    ds.store_name,
    ds.total_sales,
    ds.total_transactions,
    ds.avg_receipt,
    ds.unique_products_sold,
    COALESCE(tc.top_category, 'Не определено') AS top_category,
    dw.avg_temperature AS avg_temperature_c,
    dw.min_temperature AS min_temperature_c,
    dw.max_temperature AS max_temperature_c,
    dw.avg_humidity,
    dw.total_precipitation AS precipitation_mm,
    dw.dominant_weather AS weather_condition,
    CURRENT_TIMESTAMP AS etl_updated_at
FROM daily_sales ds
LEFT JOIN top_categories tc ON ds.store_id = tc.store_id
LEFT JOIN daily_weather dw ON ds.city_name = dw.city_name
ON CONFLICT (report_date, store_id) DO UPDATE SET
    total_sales = EXCLUDED.total_sales,
    total_transactions = EXCLUDED.total_transactions,
    avg_receipt = EXCLUDED.avg_receipt,
    unique_products_sold = EXCLUDED.unique_products_sold,
    top_category = EXCLUDED.top_category,
    avg_temperature_c = EXCLUDED.avg_temperature_c,
    min_temperature_c = EXCLUDED.min_temperature_c,
    max_temperature_c = EXCLUDED.max_temperature_c,
    avg_humidity = EXCLUDED.avg_humidity,
    precipitation_mm = EXCLUDED.precipitation_mm,
    weather_condition = EXCLUDED.weather_condition,
    etl_updated_at = EXCLUDED.etl_updated_at;

-- Возвращаем статистику для логирования
SELECT
    'mart.daily_sales_weather' AS table_name,
    COUNT(*) AS rows_affected
FROM daily_sales;