from prefect import task
from datetime import datetime

@task
def transform_hourly_data(raw_data: dict):
    city = raw_data["city"]
    hourly = raw_data["hourly"]
    
    hourly_records = []
    for i in range(len(hourly["time"])):
        try:
            date_str = hourly["time"][i][:10]
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            record = {
                "city": city,
                "date": date_obj,
                "hour": int(hourly["time"][i][11:13]),
                "temperature": hourly["temperature_2m"][i],
                "precipitation_probability": hourly["precipitation_probability"][i],
                "precipitation_mm": hourly["precipitation"][i],
                "wind_speed_kmh": hourly["wind_speed_10m"][i],
                "wind_direction_deg": hourly["wind_direction_10m"][i]
            }
            hourly_records.append(record)
        except (ValueError, IndexError, KeyError):
            continue
    
    return hourly_records

@task
def transform_daily_data(hourly_records: list):
    if not hourly_records:
        return None
    
    city = hourly_records[0]["city"]
    date_obj = hourly_records[0]["date"]
    
    temps = [r["temperature"] for r in hourly_records]
    precipitations = [r["precipitation_mm"] for r in hourly_records]
    wind_speeds = [r["wind_speed_kmh"] for r in hourly_records]
    
    daily_stats = {
        "city": city,
        "date": date_obj,
        "temp_min": min(temps),
        "temp_max": max(temps),
        "temp_avg": sum(temps) / len(temps),
        "precipitation_total_mm": sum(precipitations),
        "wind_max": max(wind_speeds) if wind_speeds else 0
    }
    
    return daily_stats