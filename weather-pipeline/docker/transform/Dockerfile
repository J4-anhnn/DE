FROM python:3.9-slim

WORKDIR /app

# Cài đặt dependencies
COPY transform/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY transform/ .

# Run the processor
CMD ["python", "weather_processor.py"]
