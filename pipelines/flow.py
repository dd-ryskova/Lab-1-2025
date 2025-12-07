from prefect import flow
from extract import fetch_weather, save_to_minio
from transform import transform_hourly_data, transform_daily_data
from load import load_to_clickhouse_hourly, load_to_clickhouse_daily
from notify import send_telegram_notification
from dotenv import load_dotenv
import traceback

load_dotenv()

@flow(name="weather_etl", log_prints=True)
def weather_etl_flow():
    try:
        cities = [
            ("Москва", 55.7558, 37.6173),
            ("Самара", 53.1959, 50.1002)
        ]
        
        for city, lat, lon in cities:
            raw_data = fetch_weather(city, lat, lon)
            save_to_minio(raw_data)
            
            hourly_records = transform_hourly_data(raw_data)
            daily_stats = transform_daily_data(hourly_records)
            
            if daily_stats:
                load_to_clickhouse_hourly(hourly_records)
                load_to_clickhouse_daily(daily_stats)
                send_telegram_notification(daily_stats)
        
    except Exception as e:
        print(f"Ошибка в пайплайне: {e}")
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    weather_etl_flow()