FROM python:3.9
WORKDIR /app

RUN python --version

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY stream.py .

RUN python --version

CMD ["faust", "-A", "stream", "worker", "-l", "info"]

###CMD ["python", "stream.py", "worker"]
