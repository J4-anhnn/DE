# Use official Python base image
FROM python:3.9-slim

# Set working directory in container
WORKDIR /app

# Copy required files
COPY requirements.txt .
COPY app.py .
COPY .env .

# Install Python dependencies
RUN pip install -r requirements.txt

# Use ARG for build-time variables
ARG PORT=8000

# Expose port from build arg
EXPOSE ${PORT}

# Run the application
CMD ["python", "app.py"]