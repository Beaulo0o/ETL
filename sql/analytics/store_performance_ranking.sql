-- =====================================================
-- Ранжирование магазинов по эффективности
-- Учитывает сезонность и погодные факторы
-- =====================================================

WITH store_metrics AS (
    SELECT
        store_id,
        store_name,
        city_name,
        DATE_TRUNC('month', report_date) AS month,
        -- Метрики эффективности
        SUM(total_sales) AS monthly_sales,
        SUM(total_transactions) AS monthly_transactions,
        AVG(avg_receipt) AS avg_monthly_receipt,
        AVG(unique_products_sold) AS avg_unique_products,
        -- Погодные факторы
        AVG(avg_temperature_c) AS avg_monthly_temp,
        SUM(precipitation_mm) AS total_precipitation,
        COUNT(DISTINCT weather_condition) AS weather_variety_days,
        -- Ранжирование внутри города
        RANK() OVER (
            PARTITION BY city_name, DATE_TRUNC('month', report_date)
            ORDER BY SUM(total_sales) DESC
        ) AS city_rank,
        -- Ранжирование общее
        RANK() OVER (
            PARTITION BY DATE_TRUNC('month', report_date)
            ORDER BY SUM(total_sales) DESC
        ) AS overall_rank
    FROM mart.daily_sales_weather
    WHERE report_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
    GROUP BY
        store_id,
        store_name,
        city_name,
        DATE_TRUNC('month', report_date)
),
performance_comparison AS (
    SELECT
        sm.*,
        -- Сравнение с предыдущим месяцем
        LAG(monthly_sales) OVER (
            PARTITION BY store_id
            ORDER BY month
        ) AS prev_month_sales,
        -- Сравнение со средним по городу
        AVG(monthly_sales) OVER (
            PARTITION BY city_name, month
        ) AS city_avg_sales,
        -- Процент от лидера в городе
        ROUND(
            monthly_sales * 100.0 / FIRST_VALUE(monthly_sales) OVER (
                PARTITION BY city_name, month
                ORDER BY monthly_sales DESC
            ),
            1
        ) AS pct_of_city_leader
    FROM store_metrics sm
)
SELECT
    month,
    city_name,
    store_name,
    ROUND(monthly_sales, 2) AS monthly_sales_rub,
    monthly_transactions,
    ROUND(avg_monthly_receipt, 2) AS avg_receipt_rub,
    city_rank,
    overall_rank,
    -- Динамика
    ROUND(monthly_sales - prev_month_sales, 2) AS sales_change_mom,
    CASE
        WHEN monthly_sales > prev_month_sales THEN '📈 Рост'
        WHEN monthly_sales < prev_month_sales THEN '📉 Падение'
        ELSE '➡️ Без изменений'
    END AS trend,
    -- Сравнение с городом
    CASE
        WHEN monthly_sales > city_avg_sales * 1.2 THEN '⭐ Выше среднего'
        WHEN monthly_sales < city_avg_sales * 0.8 THEN '⚠️ Ниже среднего'
        ELSE '✓ В пределах нормы'
    END AS vs_city_avg,
    pct_of_city_leader || '%' AS pct_of_leader,
    ROUND(avg_monthly_temp, 1) AS avg_temp_c
FROM performance_comparison
WHERE month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months')
ORDER BY month DESC, city_name, monthly_sales DESC;