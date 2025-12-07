from prefect import task
from clickhouse_driver import Client
import os

@task
def load_to_clickhouse_hourly(hourly_records: list):
    if not hourly_records:
        return
    
    city = hourly_records[0]["city"]
    
    try:
        ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
        ch_port = int(os.getenv("CLICKHOUSE_PORT", "9002"))
        ch_user = os.getenv("CLICKHOUSE_USER", "admin")
        ch_password = os.getenv("CLICKHOUSE_PASSWORD", "password123")
        ch_db = os.getenv("CLICKHOUSE_DB", "weather")
        
        client = Client(
            host=ch_host,
            port=ch_port,
            user=ch_user,
            password=ch_password,
            database=ch_db
        )
        
        data_to_insert = []
        for record in hourly_records:
            data_to_insert.append((
                record['city'],
                record['date'],
                record['hour'],
                float(record['temperature']),
                int(record['precipitation_probability']),
                float(record['precipitation_mm']),
                float(record['wind_speed_kmh']),
                int(record['wind_direction_deg'])
            ))
        
        client.execute(
            'INSERT INTO weather_hourly (city, date, hour, temperature, precipitation_probability, precipitation_mm, wind_speed_kmh, wind_direction_deg) VALUES',
            data_to_insert
        )
            
    except Exception:
        pass

@task
def load_to_clickhouse_daily(daily_stats: dict):
    if not daily_stats:
        return
    
    try:
        ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
        ch_port = int(os.getenv("CLICKHOUSE_PORT", "9002"))
        ch_user = os.getenv("CLICKHOUSE_USER", "admin")
        ch_password = os.getenv("CLICKHOUSE_PASSWORD", "password123")
        ch_db = os.getenv("CLICKHOUSE_DB", "weather")
        
        client = Client(
            host=ch_host,
            port=ch_port,
            user=ch_user,
            password=ch_password,
            database=ch_db
        )
        
        data_to_insert = [(
            daily_stats['city'],
            daily_stats['date'],
            float(daily_stats['temp_min']),
            float(daily_stats['temp_max']),
            float(daily_stats['temp_avg']),
            float(daily_stats['precipitation_total_mm']),
        )]
        
        client.execute(
            'INSERT INTO weather_daily (city, date, temp_min, temp_max, temp_avg, precipitation_total_mm) VALUES',
            data_to_insert
        )
        
    except Exception:
        pass