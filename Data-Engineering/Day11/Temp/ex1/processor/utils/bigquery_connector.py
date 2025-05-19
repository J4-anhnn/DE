from google.cloud import bigquery
import pandas as pd
import config

class BigQueryConnector:
    def __init__(self):
        # Thiết lập xác thực - nếu bạn đã cài Google Cloud SDK và đăng nhập
        self.client = bigquery.Client(project=config.BIGQUERY_PROJECT)
        
    def ensure_table_exists(self):
        """Đảm bảo dataset và table tồn tại"""
        dataset_ref = self.client.dataset(config.BIGQUERY_DATASET)
        
        # Tạo dataset nếu chưa tồn tại
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Có thể thay đổi khu vực
            self.client.create_dataset(dataset)
            print(f"Dataset {config.BIGQUERY_DATASET} đã được tạo.")
        
        # Định nghĩa schema cho bảng
        schema = [
            bigquery.SchemaField("event_time", "TIMESTAMP"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("count", "INTEGER"),
            bigquery.SchemaField("avg_duration", "FLOAT"),
        ]
        
        # Tạo bảng nếu chưa tồn tại
        table_ref = dataset_ref.table(config.BIGQUERY_TABLE)
        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            print(f"Bảng {config.BIGQUERY_TABLE} đã được tạo.")
    
    def insert_data(self, data):
        """Chèn dữ liệu vào BigQuery"""
        table_id = f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_TABLE}"
        
        # Chuyển đổi dữ liệu thành DataFrame nếu cần
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame([data])
        
        # Tải dữ liệu vào BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        
        job = self.client.load_table_from_dataframe(
            data, table_id, job_config=job_config
        )
        
        # Đợi job hoàn thành
        job.result()
        print(f"Đã chèn {len(data)} bản ghi vào {table_id}")

if __name__ == "__main__":
    # Test kết nối
    connector = BigQueryConnector()
    connector.ensure_table_exists()
    print("Đã kiểm tra kết nối BigQuery thành công!")
