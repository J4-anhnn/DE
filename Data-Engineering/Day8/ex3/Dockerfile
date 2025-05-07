FROM openjdk:11-jre

RUN apt-get update && apt-get install -y python3 python3-pip procps && apt-get clean

RUN pip3 install pyspark pandas

ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --no-cache-dir pyspark pyarrow

WORKDIR /app

COPY . /app

CMD ["python", "main.py"]

