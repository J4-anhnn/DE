
# Linux and Docker Commands

## Các lệnh Linux thường dùng

1. **`ls`**: Liệt kê các tệp và thư mục trong thư mục hiện tại.
   - `ls -l`: Hiển thị chi tiết các tệp/thư mục.
   - `ls -a`: Hiển thị tất cả các tệp, bao gồm tệp ẩn.

2. **`cd`**: Thay đổi thư mục.
   - `cd /path/to/directory`: Chuyển đến thư mục chỉ định.

3. **`pwd`**: In ra đường dẫn tuyệt đối của thư mục hiện tại.

4. **`mkdir`**: Tạo một thư mục mới.
   - `mkdir myfolder`: Tạo thư mục `myfolder`.

5. **`rm`**: Xóa tệp hoặc thư mục.
   - `rm file.txt`: Xóa tệp `file.txt`.
   - `rm -r folder/`: Xóa thư mục và các tệp con.

6. **`cp`**: Sao chép tệp hoặc thư mục.
   - `cp source.txt destination.txt`: Sao chép `source.txt` vào `destination.txt`.

7. **`mv`**: Di chuyển hoặc đổi tên tệp/thư mục.
   - `mv oldname.txt newname.txt`: Đổi tên tệp.

8. **`cat`**: Hiển thị nội dung của tệp.
   - `cat file.txt`: Hiển thị nội dung của tệp `file.txt`.

9. **`grep`**: Tìm kiếm trong tệp.
   - `grep "pattern" file.txt`: Tìm kiếm "pattern" trong `file.txt`.

10. **`chmod`**: Thay đổi quyền truy cập của tệp/thư mục.
    - `chmod 755 script.sh`: Cấp quyền cho `script.sh`.

11. **`ps`**: Hiển thị các tiến trình đang chạy.
    - `ps aux`: Hiển thị thông tin chi tiết về các tiến trình.

12. **`top`**: Hiển thị tiến trình và tài nguyên hệ thống đang sử dụng.

13. **`kill`**: Dừng tiến trình.
    - `kill PID`: Dừng tiến trình với PID cụ thể.

14. **`df`**: Hiển thị thông tin về dung lượng đĩa.
    - `df -h`: Hiển thị thông tin dung lượng đĩa theo cách dễ đọc.

15. **`du`**: Hiển thị dung lượng sử dụng của tệp/thư mục.
    - `du -sh folder/`: Hiển thị dung lượng thư mục `folder`.

16. **`ifconfig`** hoặc **`ip`**: Hiển thị thông tin mạng.
    - `ifconfig`: Hiển thị cấu hình mạng.
    - `ip a`: Hiển thị thông tin mạng.

17. **`sudo`**: Thực thi lệnh với quyền quản trị.
    - `sudo apt update`: Cập nhật danh sách gói trên Ubuntu.

18. **`apt-get`**: Quản lý gói trên Debian/Ubuntu.
    - `sudo apt-get install package_name`: Cài đặt gói.

19. **`nano`** hoặc **`vim`**: Trình soạn thảo văn bản trong terminal.
    - `nano file.txt`: Mở tệp trong `nano`.
    - `vim file.txt`: Mở tệp trong `vim`.

---

## Các lệnh Docker thường dùng

1. **`docker --version`**: Hiển thị phiên bản Docker đang sử dụng.

2. **`docker pull`**: Tải một image từ Docker Hub.
   - `docker pull ubuntu`: Tải image Ubuntu.

3. **`docker build`**: Xây dựng một image từ Dockerfile.
   - `docker build -t myimage .`: Xây dựng image với tên `myimage`.

4. **`docker images`**: Hiển thị các image đã được tải về.
   - `docker images`: Liệt kê các image.

5. **`docker ps`**: Hiển thị các container đang chạy.
   - `docker ps`: Hiển thị container đang chạy.
   - `docker ps -a`: Hiển thị tất cả các container (kể cả đã dừng).

6. **`docker run`**: Chạy một container từ image.
   - `docker run -it ubuntu bash`: Chạy container từ image Ubuntu và mở shell bash.

7. **`docker exec`**: Thực thi lệnh trong container đang chạy.
   - `docker exec -it container_id bash`: Mở terminal trong container.

8. **`docker stop`**: Dừng một container đang chạy.
   - `docker stop container_id`: Dừng container với `container_id`.

9. **`docker start`**: Bắt đầu một container đã dừng.
   - `docker start container_id`: Bắt đầu container đã dừng.

10. **`docker restart`**: Khởi động lại container.
    - `docker restart container_id`: Khởi động lại container.

11. **`docker rm`**: Xóa container.
    - `docker rm container_id`: Xóa container.

12. **`docker rmi`**: Xóa image.
    - `docker rmi image_id`: Xóa image.

13. **`docker logs`**: Hiển thị log của container.
    - `docker logs container_id`: Hiển thị log của container.

14. **`docker network ls`**: Liệt kê các network Docker.
    - `docker network ls`: Liệt kê các network.

15. **`docker-compose`**: Quản lý các dịch vụ Docker.
    - `docker-compose up`: Khởi động các dịch vụ từ file `docker-compose.yml`.
    - `docker-compose down`: Dừng và xóa các dịch vụ.

16. **`docker volume ls`**: Liệt kê các volume Docker.
    - `docker volume ls`: Liệt kê các volume.

17. **`docker info`**: Hiển thị thông tin chi tiết về Docker.
    - `docker info`: Hiển thị thông tin về Docker engine và containers.

18. **`docker stats`**: Hiển thị thống kê tài nguyên của các container đang chạy.
    - `docker stats`: Thống kê tài nguyên của container.

