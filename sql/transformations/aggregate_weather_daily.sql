-- =====================================================
-- Агрегация сырых погодных данных в дневную таблицу
-- Запускается ежедневно после сбора погоды
-- =====================================================

WITH weather_aggregated AS (
    SELECT
        DATE(extraction_timestamp) AS date,
        city_name,
        ROUND(AVG(extracted_temperature)::NUMERIC, 2) AS avg_temperature,
        MIN(extracted_temperature) AS min_temperature,
        MAX(extracted_temperature) AS max_temperature,
        ROUND(AVG(extracted_humidity)) AS avg_humidity,
        ROUND(AVG(extracted_pressure)) AS avg_pressure,
        COUNT(*) AS measurements_count,
        -- Определение доминирующей погоды (мода)
        MODE() WITHIN GROUP (ORDER BY weather_main) AS dominant_weather
    FROM staging.weather_raw
    WHERE DATE(extraction_timestamp) = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY DATE(extraction_timestamp), city_name
)
INSERT INTO ods.weather_daily (
    date,
    city_name,
    avg_temperature,
    min_temperature,
    max_temperature,
    avg_humidity,
    avg_pressure,
    total_precipitation,
    dominant_weather,
    measurements_count,
    etl_created_at
)
SELECT
    date,
    city_name,
    avg_temperature,
    min_temperature,
    max_temperature,
    avg_humidity,
    avg_pressure,
    0 AS total_precipitation, -- Будет заполнено отдельно из API прогноза
    dominant_weather,
    measurements_count,
    CURRENT_TIMESTAMP
FROM weather_aggregated
ON CONFLICT (date, city_name) DO UPDATE SET
    avg_temperature = EXCLUDED.avg_temperature,
    min_temperature = EXCLUDED.min_temperature,
    max_temperature = EXCLUDED.max_temperature,
    avg_humidity = EXCLUDED.avg_humidity,
    avg_pressure = EXCLUDED.avg_pressure,
    dominant_weather = EXCLUDED.dominant_weather,
    measurements_count = EXCLUDED.measurements_count,
    etl_created_at = CURRENT_TIMESTAMP;

-- Удаление сырых данных старше 30 дней (очистка)
DELETE FROM staging.weather_raw
WHERE extraction_timestamp < CURRENT_DATE - INTERVAL '30 days';