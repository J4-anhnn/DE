import os
import pandas as pd
import asyncio
import asyncpg
from dotenv import load_dotenv
from google.cloud import storage
import csv
from io import StringIO

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# GCP Storage parameters
BUCKET_NAME = os.getenv("BUCKET_NAME", "training-de-457603_cloudbuild")
PROJECT_ID = os.getenv("PROJECT_ID", "training-de-457603")

async def get_db_connection():
    """Get a database connection"""
    return await asyncpg.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )

def extract_data(file_path):
    """Extract data from CSV file"""
    try:
        print(f"Extracting data from {file_path}")
        # Read CSV file into pandas DataFrame
        df = pd.read_csv(file_path)
        print(f"Extracted {len(df)} rows of data")
        print("Columns:", df.columns.tolist())  # Debug: print column names
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None

def transform_data(df):
    """Transform the data"""
    try:
        print("Transforming data...")
        
        # Ensure date column is in datetime format
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            # Extract month and year
            df['month'] = df['Date'].dt.month
            df['year'] = df['Date'].dt.year
        
        # Calculate monthly revenue based on Forecasted Monthly Revenue
        if 'Forecasted Monthly Revenue' in df.columns and 'month' in df.columns and 'year' in df.columns:
            monthly_revenue = df.groupby(['year', 'month'])['Forecasted Monthly Revenue'].sum().reset_index()
            monthly_revenue.rename(columns={'Forecasted Monthly Revenue': 'monthly_revenue'}, inplace=True)
            
            # Filter out low-performing months (e.g., below average)
            avg_revenue = monthly_revenue['monthly_revenue'].mean()
            filtered_months = monthly_revenue[monthly_revenue['monthly_revenue'] >= avg_revenue]
            
            print(f"Transformed data: {len(filtered_months)} months after filtering")
            print("Sample data:", filtered_months.head())  # Debug: print sample data
            return filtered_months
        else:
            print("Required columns not found in the dataset")
            print("Available columns:", df.columns.tolist())
            return None
    except Exception as e:
        print(f"Error transforming data: {e}")
        return None

def load_to_cloud_storage(df, destination_blob_name):
    """Upload transformed data to GCP Cloud Storage"""
    try:
        print(f"Uploading data to Cloud Storage: {destination_blob_name}")
        
        # Convert DataFrame to CSV string
        csv_data = StringIO()
        df.to_csv(csv_data, index=False)
        
        # Initialize GCP client
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        
        # Upload the CSV data
        blob.upload_from_string(csv_data.getvalue(), content_type="text/csv")
        
        print(f"Data successfully uploaded to gs://{BUCKET_NAME}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f"Error uploading to Cloud Storage: {e}")
        return False

async def load_to_database(df, table_name="monthly_revenue"):
    """Load transformed data to PostgreSQL database"""
    conn = await get_db_connection()
    try:
        print(f"Loading data to database table: {table_name}")
        
        # Create table if it doesn't exist
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                monthly_revenue NUMERIC NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert data
        records = 0
        for _, row in df.iterrows():
            await conn.execute(f'''
                INSERT INTO {table_name} (year, month, monthly_revenue)
                VALUES ($1, $2, $3)
            ''', int(row['year']), int(row['month']), float(row['monthly_revenue']))
            records += 1
        
        print(f"Successfully loaded {records} records to database")
        return True
    except Exception as e:
        print(f"Error loading data to database: {e}")
        return False
    finally:
        await conn.close()

async def main():
    # File path for the CSV data
    file_path = "data/sales_data.csv"
    
    # Extract
    df = extract_data(file_path)
    if df is None:
        print("Failed to extract data")
        return
    
    # Transform
    transformed_df = transform_data(df)
    if transformed_df is None or transformed_df.empty:
        print("No data to load after transformation")
        return
    
    # Load to Cloud Storage
    destination_blob = "transformed_data/monthly_revenue.csv"
    cloud_upload_success = load_to_cloud_storage(transformed_df, destination_blob)
    
    # Load to Database
    db_load_success = await load_to_database(transformed_df)
    
    if cloud_upload_success and db_load_success:
        print("ETL process completed successfully")
    else:
        print("ETL process completed with errors")

if __name__ == "__main__":
    asyncio.run(main())
