"""
Kafka consumer for weather alerts
"""
import json
import os
import logging
from kafka import KafkaConsumer
import smtplib
from email.message import EmailMessage
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Alert thresholds
ALERT_THRESHOLDS = {
    "temperature_high": 35.0,  # Celsius
    "temperature_low": 5.0,    # Celsius
    "wind_speed_high": 15.0,   # m/s
    "humidity_high": 90.0,     # percentage
    "pressure_low": 990.0,     # hPa
    "pressure_high": 1030.0    # hPa
}

# Email configuration
EMAIL_ENABLED = os.getenv("EMAIL_ALERTS_ENABLED", "false").lower() == "true"
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "weather-alerts@example.com")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "admin@example.com").split(",")
EMAIL_SERVER = os.getenv("EMAIL_SERVER", "localhost")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "25"))
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")

def create_kafka_consumer():
    """Create Kafka consumer"""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    consumer = KafkaConsumer(
        "weather-events",
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="weather-alerts-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    
    logger.info(f"Connected to Kafka at {bootstrap_servers}")
    return consumer

def check_alerts(data):
    """Check if weather data exceeds alert thresholds"""
    alerts = []
    
    # Check temperature
    if data.get("temperature") > ALERT_THRESHOLDS["temperature_high"]:
        alerts.append(f"HIGH TEMPERATURE ALERT: {data['city']} has temperature of {data['temperature']}°C")
    elif data.get("temperature") < ALERT_THRESHOLDS["temperature_low"]:
        alerts.append(f"LOW TEMPERATURE ALERT: {data['city']} has temperature of {data['temperature']}°C")
    
    # Check wind speed
    if data.get("wind_speed") > ALERT_THRESHOLDS["wind_speed_high"]:
        alerts.append(f"HIGH WIND ALERT: {data['city']} has wind speed of {data['wind_speed']} m/s")
    
    # Check humidity
    if data.get("humidity") > ALERT_THRESHOLDS["humidity_high"]:
        alerts.append(f"HIGH HUMIDITY ALERT: {data['city']} has humidity of {data['humidity']}%")
    
    # Check pressure
    if data.get("pressure") < ALERT_THRESHOLDS["pressure_low"]:
        alerts.append(f"LOW PRESSURE ALERT: {data['city']} has pressure of {data['pressure']} hPa")
    elif data.get("pressure") > ALERT_THRESHOLDS["pressure_high"]:
        alerts.append(f"HIGH PRESSURE ALERT: {data['city']} has pressure of {data['pressure']} hPa")
    
    return alerts

def send_email_alert(subject, body):
    """Send email alert"""
    if not EMAIL_ENABLED:
        logger.info(f"Email alerts disabled. Would have sent: {subject}")
        return
    
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = ", ".join(EMAIL_RECIPIENTS)
        
        with smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT) as server:
            if EMAIL_USERNAME and EMAIL_PASSWORD:
                server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.send_message(msg)
        
        logger.info(f"Sent email alert: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")

def process_alerts():
    """Main function to process weather alerts"""
    consumer = create_kafka_consumer()
    
    try:
        for message in consumer:
            data = message.value
            logger.debug(f"Received message: {data}")
            
            # Check for alerts
            alerts = check_alerts(data)
            
            # Process alerts
            for alert in alerts:
                logger.warning(alert)
                
                # Send email for critical alerts
                if "HIGH TEMPERATURE" in alert or "HIGH WIND" in alert:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    subject = f"WEATHER ALERT: {data['city']} - {timestamp}"
                    body = f"""
                    {alert}
                    
                    Weather details:
                    - City: {data['city']}
                    - Temperature: {data.get('temperature')}°C
                    - Humidity: {data.get('humidity')}%
                    - Pressure: {data.get('pressure')} hPa
                    - Wind Speed: {data.get('wind_speed')} m/s
                    - Condition: {data.get('weather_condition')}
                    - Time: {timestamp}
                    
                    This is an automated alert from the Weather Monitoring System.
                    """
                    send_email_alert(subject, body)
    
    except KeyboardInterrupt:
        logger.info("Stopping alert processor")
    finally:
        consumer.close()

if __name__ == "__main__":
    process_alerts()
