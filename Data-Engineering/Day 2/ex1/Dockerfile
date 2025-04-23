
# Use official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Run the ETL script
ENV PYTHONPATH="${PYTHONPATH}:/app"
CMD ["python", "src/etl_pipeline.py"]
