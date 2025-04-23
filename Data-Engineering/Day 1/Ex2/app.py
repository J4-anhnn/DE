from flask import Flask
from bigquery import query_bigquery
app = Flask(__name__)

@app.route("/")
def home():
    return "Hello, World!"

@app.route("/bigquery")
def bq():
    return query_bigquery()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
