FROM openjdk:11-jre-slim

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    apt-get clean

RUN wget -P /opt/ https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY . .

ENV PYSPARK_SUBMIT_ARGS="--jars /opt/postgresql-42.7.1.jar pyspark-shell"

CMD ["python3", "main.py"]
