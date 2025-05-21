FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir streamlit pandas plotly google-cloud-bigquery

# Copy source code
COPY dashboard/ ./dashboard/
COPY config/ ./config/

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
