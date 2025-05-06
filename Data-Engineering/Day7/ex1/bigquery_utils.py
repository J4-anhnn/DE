from google.cloud import bigquery
from datetime import datetime
import os

def get_bigquery_client():
    return bigquery.Client()

def get_sales_data_schema():
    """Get schema for sales data table with partitioning."""
    return [
        bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),  # Partitioning field
        bigquery.SchemaField("Salesperson", "STRING"),  # Sales representative
        bigquery.SchemaField("Lead Name", "STRING"),  # Lead name
        bigquery.SchemaField("Segment", "STRING"),  # Market segment
        bigquery.SchemaField("Region", "STRING"),  # Region
        bigquery.SchemaField("Target Close", "DATE"),  # Expected closing date
        bigquery.SchemaField("Forecasted Monthly Revenue", "FLOAT"),  # Expected monthly revenue
        bigquery.SchemaField("Opportunity Stage", "STRING"),  # Opportunity stage
        bigquery.SchemaField("Weighted Revenue", "FLOAT"),  # Weighted revenue
        bigquery.SchemaField("Closed Opportunity", "BOOLEAN"),  # Closed opportunity
        bigquery.SchemaField("Active Opportunity", "BOOLEAN"),  # Active opportunity
        bigquery.SchemaField("Latest Status Entry", "TIMESTAMP")  # Latest status update
    ]

def get_or_create_dataset(client, dataset_id, location="US"):
    """Get or create BigQuery dataset."""
    dataset_ref = client.dataset(dataset_id)
    try:
        return client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        return client.create_dataset(dataset)

def get_or_create_partitioned_table(client, dataset_id, table_id):
    """Get or create partitioned table."""
    dataset_ref = get_or_create_dataset(client, dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        return client.get_table(table_ref)
    except Exception:
        schema = get_sales_data_schema()
        table = bigquery.Table(table_ref, schema=schema)
        
        # Configure partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="Date"  # Partition by Date field
        )
        
        # Add clustering for common query patterns
        table.clustering_fields = ["Region", "Salesperson"]
        
        return client.create_table(table)

def load_dataframe_to_bq(df, client, dataset_id, table_id):
    """Load DataFrame to BigQuery with partitioning."""
    table = get_or_create_partitioned_table(client, dataset_id, table_id)
    
    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
        allow_quoted_newlines=True,
    )
    
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()  # Wait for job to complete
    
    return client.get_table(table)
