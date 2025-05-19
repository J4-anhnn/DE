#!/bin/bash

# Tham số
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
TOPIC=${KAFKA_TOPIC:-click_stream_data}

# Màu cho output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===== Kiểm tra và Tiến hóa Schema Avro =====${NC}"

# Kiểm tra tính tương thích của schema V2
echo -e "\n${YELLOW}Kiểm tra tính tương thích của schema V2...${NC}"
python schema_compatibility_test.py schemas/v2/click_event.avsc check
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Schema V2 tương thích với schema hiện tại!${NC}"
    
    # Đăng ký schema v2
    echo -e "\n${YELLOW}Đăng ký schema V2 với Schema Registry...${NC}"
    python schema_compatibility_test.py schemas/v2/click_event.avsc register
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Schema V2 đã được đăng ký thành công!${NC}"
        
        # Cập nhật cấu hình để sử dụng schema V2
        echo -e "\n${YELLOW}Cập nhật Producer và Processor để sử dụng schema V2...${NC}"
        
        # Cập nhật biến môi trường
        export SCHEMA_VERSION=2
        
        echo -e "${GREEN}Đã cập nhật SCHEMA_VERSION=2 trong biến môi trường.${NC}"
        echo -e "${YELLOW}Khởi động lại các services để áp dụng schema mới...${NC}"
        echo -e "Thực hiện: docker-compose restart producer processor"
    else
        echo -e "${RED}Không thể đăng ký schema V2. Kiểm tra logs để biết thêm chi tiết.${NC}"
    fi
else
    echo -e "${RED}Schema V2 KHÔNG tương thích với schema hiện tại.${NC}"
        echo -e "${YELLOW}Gợi ý: Xem xét các quy tắc tương thích schema và điều chỉnh schema hoặc thay đổi chế độ tương thích.${NC}"
    echo -e "${YELLOW}Các chế độ tương thích:${NC}"
    echo -e "  - BACKWARD: Consumers mới có thể đọc dữ liệu cũ (thêm trường optional, không xóa trường)"
    echo -e "  - FORWARD: Consumers cũ có thể đọc dữ liệu mới (xóa trường optional, không thêm trường required)"
    echo -e "  - FULL: Cả BACKWARD và FORWARD"
    echo -e "  - NONE: Không kiểm tra tương thích"
fi

echo -e "\n${YELLOW}===== Kiểm tra tình trạng Schema Registry =====${NC}"
curl -s $SCHEMA_REGISTRY_URL/subjects | jq
echo -e "\n${YELLOW}Chi tiết các phiên bản cho subject:${NC}"
curl -s $SCHEMA_REGISTRY_URL/subjects/$TOPIC-value/versions | jq

echo -e "\n${YELLOW}===== Hoàn thành! =====${NC}"
