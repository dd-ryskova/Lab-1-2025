-- Создание базы данных weather
CREATE DATABASE IF NOT EXISTS weather;

-- Таблица для почасовых данных прогноза
CREATE TABLE IF NOT EXISTS weather.weather_hourly
(
    city String,
    date Date,
    hour UInt8,
    temperature Float32,
    precipitation_probability UInt8,
    precipitation_mm Float32,
    wind_speed_kmh Float32,
    wind_direction_deg UInt16,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (city, date, hour);

-- Таблица для агрегированных дневных данных
CREATE TABLE IF NOT EXISTS weather.weather_daily
(
    city String,
    date Date,
    temp_min Float32,
    temp_max Float32,
    temp_avg Float32,
    precipitation_total_mm Float32,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (city, date);