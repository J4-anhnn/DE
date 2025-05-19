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
        
        # Xác định schema dựa trên phiên bản
        schema_fields = [
            bigquery.SchemaField("event_time", "TIMESTAMP"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("count", "INTEGER"),
            bigquery.SchemaField("avg_duration", "FLOAT"),
        ]
        
        # Thêm trường từ schema v2 nếu đang sử dụng
        if config.SCHEMA_VERSION >= 2:
            schema_fields.extend([
                bigquery.SchemaField("browser", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("os", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("device_type", "STRING", mode="NULLABLE"),
            ])
        
        # Tạo bảng nếu chưa tồn tại
        table_ref = dataset_ref.table(config.BIGQUERY_TABLE)
        try:
            existing_table = self.client.get_table(table_ref)
            
            # Kiểm tra và thêm các trường mới nếu cần
            existing_fields = [field.name for field in existing_table.schema]
            new_fields = []
            
            for field in schema_fields:
                if field.name not in existing_fields:
                    new_fields.append(field)
            
            # Cập nhật schema nếu có trường mới
            if new_fields:
                print(f"Cập nhật schema bảng với các trường mới: {', '.join([f.name for f in new_fields])}")
                updated_schema = existing_table.schema.copy()
                for field in new_fields:
                    updated_schema.append(field)
                
                existing_table.schema = updated_schema
                self.client.update_table(existing_table, ["schema"])
                print(f"Đã cập nhật schema bảng {config.BIGQUERY_TABLE}")
            
        except Exception:
            table = bigquery.Table(table_ref, schema=schema_fields)
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
