-- =====================================================
-- Аналитический запрос: Корреляция продаж и погоды
-- Для анализа в BI инструментах
-- =====================================================

WITH sales_weather_stats AS (
    SELECT
        city_name,
        store_name,
        weather_condition,
        AVG(total_sales) AS avg_daily_sales,
        AVG(total_transactions) AS avg_transactions,
        AVG(avg_temperature_c) AS avg_temperature,
        COUNT(*) AS days_count,
        -- Корреляция Пирсона между температурой и продажами
        CORR(total_sales, avg_temperature_c) AS temp_sales_correlation
    FROM mart.daily_sales_weather
    WHERE report_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY city_name, store_name, weather_condition
),
weather_impact_analysis AS (
    SELECT
        city_name,
        store_name,
        weather_condition,
        avg_daily_sales,
        avg_transactions,
        avg_temperature,
        days_count,
        temp_sales_correlation,
        CASE
            WHEN temp_sales_correlation > 0.5 THEN 'Сильная положительная корреляция'
            WHEN temp_sales_correlation > 0.2 THEN 'Умеренная положительная корреляция'
            WHEN temp_sales_correlation < -0.5 THEN 'Сильная отрицательная корреляция'
            WHEN temp_sales_correlation < -0.2 THEN 'Умеренная отрицательная корреляция'
            ELSE 'Слабая корреляция'
        END AS correlation_strength
    FROM sales_weather_stats
    WHERE days_count >= 5  
)
SELECT
    city_name,
    store_name,
    weather_condition,
    ROUND(avg_daily_sales, 2) AS avg_daily_sales_rub,
    avg_transactions,
    ROUND(avg_temperature, 1) AS avg_temperature_c,
    days_count,
    ROUND(temp_sales_correlation, 3) AS temp_sales_correlation,
    correlation_strength,
    CASE
        WHEN temp_sales_correlation > 0.3 AND weather_condition = 'Clear' THEN 'Увеличить запасы в солнечные дни'
        WHEN temp_sales_correlation < -0.3 AND weather_condition = 'Rain' THEN 'Сократить заказы скоропортящихся товаров'
        WHEN ABS(temp_sales_correlation) > 0.7 THEN 'Сильная зависимость - использовать погодный фактор в прогнозировании'
        ELSE 'Нет явной зависимости'
    END AS recommendation
FROM weather_impact_analysis
ORDER BY city_name, store_name, ABS(temp_sales_correlation) DESC;
