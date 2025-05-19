FROM python:3.9-slim

WORKDIR /app

# Cài đặt dependencies
COPY load/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY load/ .

# Run the loader
CMD ["python", "bigquery_loader.py"]
