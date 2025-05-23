#!/usr/bin/env python3
import os
import time
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Weather processor started")
    
    # Trong thực tế, đây sẽ là code xử lý dữ liệu với Spark
    # Hiện tại chỉ là placeholder
    while True:
        logger.info("Processor running...")
        time.sleep(60)

if __name__ == "__main__":
    main()
