FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install confluent-kafka

CMD ["python", "main.py"]
