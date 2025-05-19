FROM python:3.9-slim

WORKDIR /app

# Cài đặt dependencies
COPY extract/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY extract/ .

# Run the collector
CMD ["python", "weather_collector.py"]