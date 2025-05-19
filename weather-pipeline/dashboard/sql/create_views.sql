-- View cho dữ liệu thời tiết hiện tại
CREATE OR REPLACE VIEW `${project_id}.${dataset_id}.current_weather_view` AS
SELECT
  city_name,
  temperature,
  feels_like,
  humidity,
  pressure,
  wind_speed,
  wind_direction,
  cloudiness,
  weather_condition,
  weather_description,
  measurement_time,
  ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY measurement_time DESC) as row_num
FROM
  `${project_id}.${dataset_id}.${batch_table}`
WHERE
  DATE(measurement_time) = CURRENT_DATE();

-- View cho dữ liệu thời tiết theo ngày
CREATE OR REPLACE VIEW `${project_id}.${dataset_id}.daily_weather_view` AS
SELECT
  city_name,
  DATE(measurement_time) as date,
  AVG(temperature) as avg_temp,
  MAX(temperature) as max_temp,
  MIN(temperature) as min_temp,
  AVG(humidity) as avg_humidity,
  AVG(pressure) as avg_pressure,
  AVG(wind_speed) as avg_wind_speed,
  COUNT(*) as measurement_count
FROM
  `${project_id}.${dataset_id}.${batch_table}`
WHERE
  DATE(measurement_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY
  city_name, DATE(measurement_time)
ORDER BY
  city_name, date;

-- View cho phân tích xu hướng
CREATE OR REPLACE VIEW `${project_id}.${dataset_id}.weather_trends_view` AS
WITH daily_data AS (
  SELECT
    city_name,
    DATE(measurement_time) as date,
    AVG(temperature) as avg_temp,
    AVG(humidity) as avg_humidity,
    AVG(pressure) as avg_pressure,
    AVG(wind_speed) as avg_wind_speed
  FROM
    `${project_id}.${dataset_id}.${batch_table}`
  WHERE
    DATE(measurement_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY
    city_name, DATE(measurement_time)
)
SELECT
  city_name,
  date,
  avg_temp,
  avg_humidity,
  avg_pressure,
  avg_wind_speed,
  AVG(avg_temp) OVER (
    PARTITION BY city_name
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as temp_7day_moving_avg,
  CASE
    WHEN AVG(avg_temp) OVER (
      PARTITION BY city_name
      ORDER BY date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) > AVG(avg_temp) OVER (
      PARTITION BY city_name
      ORDER BY date
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) THEN 'increasing'
    WHEN AVG(avg_temp) OVER (
      PARTITION BY city_name
      ORDER BY date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) < AVG(avg_temp) OVER (
      PARTITION BY city_name
      ORDER BY date
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) THEN 'decreasing'
    ELSE 'stable'
  END as temp_trend
FROM
  daily_data
ORDER BY
  city_name, date;

-- View cho cảnh báo thời tiết
CREATE OR REPLACE VIEW `${project_id}.${dataset_id}.weather_alerts_view` AS
SELECT
  city_name,
  CASE
    WHEN temperature > 35 THEN 'HIGH_TEMPERATURE'
    WHEN temperature < 5 THEN 'LOW_TEMPERATURE'
    WHEN wind_speed > 15 THEN 'HIGH_WIND'
    WHEN humidity > 90 THEN 'HIGH_HUMIDITY'
    ELSE 'OTHER'
  END as alert_type,
  CASE
    WHEN temperature > 38 OR temperature < 0 OR wind_speed > 20 THEN 'critical'
    WHEN temperature > 35 OR temperature < 5 OR wind_speed > 15 OR humidity > 90 THEN 'warning'
    ELSE 'info'
  END as alert_level,
  temperature,
  humidity,
  wind_speed,
  pressure,
  measurement_time
FROM
  `${project_id}.${dataset_id}.${batch_table}`
WHERE
  measurement_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND (temperature > 35 OR temperature < 5 OR wind_speed > 15 OR humidity > 90)
ORDER BY
  measurement_time DESC;

-- View cho dữ liệu real-time
CREATE OR REPLACE VIEW `${project_id}.${dataset_id}.realtime_weather_view` AS
SELECT
  city,
  avg_temperature,
  max_temperature,
  min_temperature,
  avg_humidity,
  avg_pressure,
  avg_wind_speed,
  window.start as window_start,
  window.end as window_end
FROM
  `${project_id}.${dataset_id}.${realtime_table}`
WHERE
  window.start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
ORDER BY
  window_start DESC;
