FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY extract/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY extract/ ./extract/
COPY config/ ./config/

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run the collector
CMD ["python", "-m", "extract.weather_collector"]
