import os
from google.cloud import bigquery

def query_bigquery():
    client = bigquery.Client()
    query = f"SELECT name FROM `{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}` LIMIT 5"
    query_job = client.query(query)
    result = [row['name'] for row in query_job]
    return {"results": result}
