import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

def get_data_from_postgres():
    """Truy vấn và lấy dữ liệu từ PostgreSQL sử dụng SQLAlchemy"""
    try:
        # Tạo connection string và engine
        conn_string = "postgresql://flinkuser:flinkpassword@localhost:5432/flinkdb"
        engine = create_engine(conn_string)
        
        # Truy vấn dữ liệu
        query = "SELECT * FROM raw_sensors_data"
        df = pd.read_sql_query(query, engine)
        
        print(f"Đã đọc {len(df)} dòng dữ liệu từ PostgreSQL")
        return df
    except Exception as e:
        print(f"Lỗi khi truy vấn dữ liệu: {e}")
        return None

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Tải dữ liệu lên BigQuery"""
    try:
        # Khởi tạo client BigQuery
        client = bigquery.Client(project=project_id)
        
        # Xác định bảng đích
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Cấu hình job load
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        
        # Tải dữ liệu lên
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        
        print(f"Đã tải {len(df)} dòng lên BigQuery thành công!")
        return True
    except Exception as e:
        print(f"Lỗi khi tải lên BigQuery: {e}")
        return False

def main():
    # Cấu hình
    project_id = "training-de-457603"
    dataset_id = "bigquery_train"
    table_id = "raw_sensors_data"
    
    # Thực hiện quy trình
    df = get_data_from_postgres()
    if df is not None and not df.empty:
        upload_to_bigquery(df, project_id, dataset_id, table_id)

if __name__ == "__main__":
    main()
