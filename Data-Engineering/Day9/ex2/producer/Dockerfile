FROM python:3.9
WORKDIR /app

RUN python --version

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY producer.py .

RUN python --version

CMD ["python", "producer.py"]
