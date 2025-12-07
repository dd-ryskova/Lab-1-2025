from prefect import task
from botocore.client import Config
from datetime import datetime
import requests
import json
import boto3
import os

@task
def fetch_weather(city: str, lat: float, lon: float) -> dict:
    url = os.getenv("OPENMETEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation_probability,precipitation,wind_speed_10m,wind_direction_10m",
        "forecast_days": 2,
        "timezone": "auto"
    }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    data["city"] = city
    data["fetched_at"] = datetime.now().isoformat()
    
    hourly = data["hourly"]
    if len(hourly["time"]) >= 48:
        for key in hourly:
            hourly[key] = hourly[key][24:48]
    
    return data

@task
def save_to_minio(data: dict):
    city = data["city"]
    
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "43214321")
    minio_bucket = os.getenv("MINIO_BUCKET", "raw-weather")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version='s3v4')
    )
    
    try:
        s3_client.head_bucket(Bucket=minio_bucket)
    except:
        s3_client.create_bucket(Bucket=minio_bucket)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{city}/{date_str}.json"
    json_data = json.dumps(data, indent=2)
    
    s3_client.put_object(
        Bucket=minio_bucket,
        Key=file_name,
        Body=json_data,
        ContentType='application/json'
    )