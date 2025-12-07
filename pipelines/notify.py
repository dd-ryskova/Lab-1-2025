from prefect import task
import requests
import os

@task
def send_telegram_notification(daily_stats: dict):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        return
    
    city = daily_stats["city"]
    date = daily_stats["date"]
    
    message = f"""
🌤 *Прогноз погоды на завтра*
📍 *Город:* {daily_stats['city']}
📅 *Дата:* {daily_stats['date']}

🌡 *Температура:*
   • Минимум: {daily_stats['temp_min']:.1f}°C
   • Максимум: {daily_stats['temp_max']:.1f}°C  
   • Средняя: {daily_stats['temp_avg']:.1f}°C

💧 *Осадки:* {daily_stats['precipitation_total_mm']:.1f} мм
"""
    
    warnings = []
    if daily_stats['precipitation_total_mm'] > 10:
        warnings.append("🌧️ Сильные осадки!")
    if daily_stats.get('wind_max', 0) > 30:
        warnings.append("💨 Сильный ветер!")
    
    if warnings:
        message += "\n⚠️ *Предупреждения:*\n" + "\n".join(warnings)
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=params, timeout=10)
    except Exception:
        pass