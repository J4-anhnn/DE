#!/usr/bin/env python
import json
import requests
import os
import sys

def load_schema(schema_path):
    """Tải schema từ file"""
    with open(schema_path, 'r') as f:
        return json.load(f)

def check_compatibility(schema_registry_url, topic, schema_path, compatibility_mode="BACKWARD"):
    """Kiểm tra tính tương thích của schema mới so với schema hiện tại"""
    # Tải schema mới
    new_schema = load_schema(schema_path)
    schema_str = json.dumps(new_schema)
    
    # Chuẩn bị subject name cho schema registry
    subject = f"{topic}-value"  # Convention của Schema Registry
    
    # Tạo payload cho kiểm tra tương thích
    payload = {"schema": schema_str}
    
    # Endpoint để kiểm tra tương thích
    url = f"{schema_registry_url}/compatibility/subjects/{subject}/versions/latest"
    
    # Thiết lập mode tương thích (nếu cần)
    # Các giá trị có thể: BACKWARD, FORWARD, FULL, NONE
    config_url = f"{schema_registry_url}/config/{subject}"
    config_payload = {"compatibility": compatibility_mode}
    try:
        config_response = requests.put(config_url, json=config_payload)
        if config_response.status_code not in (200, 201, 204):
            print(f"Cảnh báo: Không thể cập nhật chế độ tương thích: {config_response.text}")
    except Exception as e:
        print(f"Lỗi khi cập nhật chế độ tương thích: {str(e)}")
    
    # Gửi request kiểm tra tương thích
    try:
        response = requests.post(url, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            is_compatible = result.get("is_compatible", False)
            
            if is_compatible:
                print(f"Schema mới tương thích với schema hiện tại!")
                return True
            else:
                print(f"Schema mới KHÔNG tương thích với schema hiện tại!")
                print("Gợi ý: Đảm bảo rằng các thay đổi tuân theo quy tắc tương thích.")
                print("- Với BACKWARD: Có thể thêm trường mới với giá trị mặc định, không xóa trường cũ.")
                print("- Với FORWARD: Có thể xóa trường không bắt buộc, không thêm trường bắt buộc mới.")
                print("- Với FULL: Tuân theo cả quy tắc BACKWARD và FORWARD.")
                return False
        else:
            print(f"Lỗi kiểm tra tương thích: {response.status_code}, {response.text}")
            
            # Nếu schema chưa tồn tại, lỗi này sẽ xuất hiện
            if response.status_code == 404 and "Subject not found" in response.text:
                print("Chưa có schema trước đó. Đây sẽ là schema đầu tiên.")
                return True
                
            return False
            
    except Exception as e:
        print(f"Lỗi khi kết nối đến Schema Registry: {str(e)}")
        return False

def register_schema(schema_registry_url, topic, schema_path):
    """Đăng ký schema mới với Schema Registry"""
    # Tải schema
    new_schema = load_schema(schema_path)
    schema_str = json.dumps(new_schema)
    
    # Chuẩn bị subject name
    subject = f"{topic}-value"
    
    # Tạo payload cho đăng ký
    payload = {"schema": schema_str}
    
    # Endpoint đăng ký schema
    url = f"{schema_registry_url}/subjects/{subject}/versions"
    
    # Gửi request đăng ký
    try:
        response = requests.post(url, json=payload)
        
        if response.status_code in (200, 201):
            result = response.json()
            schema_id = result.get("id")
            print(f"Schema đã được đăng ký thành công với ID: {schema_id}")
            return True
        else:
            print(f"Lỗi đăng ký schema: {response.status_code}, {response.text}")
            return False
            
    except Exception as e:
        print(f"Lỗi khi kết nối đến Schema Registry: {str(e)}")
        return False

def main():
    # Lấy tham số từ dòng lệnh hoặc sử dụng giá trị mặc định
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("KAFKA_TOPIC", "click_stream_data")
    compatibility_mode = os.getenv("COMPATIBILITY_MODE", "BACKWARD")
    
    if len(sys.argv) < 2:
        print("Sử dụng: python schema_compatibility_test.py <schema_path> [action]")
        print("  schema_path: Đường dẫn đến file schema Avro")
        print("  action: 'check' (mặc định) hoặc 'register'")
        sys.exit(1)
    
    schema_path = sys.argv[1]
    action = sys.argv[2] if len(sys.argv) > 2 else "check"
    
    if action.lower() == "check":
        # Chỉ kiểm tra tương thích
        is_compatible = check_compatibility(schema_registry_url, topic, schema_path, compatibility_mode)
        sys.exit(0 if is_compatible else 1)
    elif action.lower() == "register":
        # Kiểm tra tương thích và đăng ký nếu OK
        is_compatible = check_compatibility(schema_registry_url, topic, schema_path, compatibility_mode)
        if is_compatible:
            success = register_schema(schema_registry_url, topic, schema_path)
            sys.exit(0 if success else 1)
        else:
            sys.exit(1)
    else:
        print(f"Hành động không hợp lệ: {action}")
        print("Hành động hợp lệ: 'check' hoặc 'register'")
        sys.exit(1)

if __name__ == "__main__":
    main()
