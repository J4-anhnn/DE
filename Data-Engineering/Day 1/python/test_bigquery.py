from google.cloud import bigquery

def test_bigquery():
    """Test BigQuery API với các thao tác cơ bản"""
    try:
        client = bigquery.Client()

        dataset_id = f"{client.project}.test_dataset"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-southeast1"
        
        print(f"Creating dataset: {dataset_id}")
        dataset = client.create_dataset(dataset, exists_ok=True)
        
        schema = [
            bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
        ]
        
        table_id = f"{dataset_id}.test_table"
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        print(f"Created table: {table_id}")
        
        rows_to_insert = [
            {"full_name": "John Doe", "age": 25},
            {"full_name": "Jane Smith", "age": 30},
        ]
        
        errors = client.insert_rows_json(table, rows_to_insert)
        if not errors:
            print("Inserted 2 rows successfully")
        else:
            print(f"Errors: {errors}")
            
        query = f"""
            SELECT full_name, age
            FROM `{table_id}`
            ORDER BY age DESC
        """
        
        print("\nQuerying data:")
        query_job = client.query(query)
        
        for row in query_job:
            print(f"Name: {row.full_name}, Age: {row.age}")
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    print("Testing BigQuery API...")
    test_bigquery() 