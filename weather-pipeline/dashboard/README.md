# Weather Dashboard - Looker Studio

## Thiết lập Dashboard

### Bước 1: Chuẩn bị dữ liệu

1. Chạy script để tạo các view trong BigQuery:

./setup_dashboard.sh


2. Xác nhận các view đã được tạo:
- current_weather_view
- daily_weather_view
- weather_trends_view
- weather_alerts_view
- realtime_weather_view

### Bước 2: Tạo Looker Studio Dashboard

1. Truy cập [Looker Studio](https://lookerstudio.google.com/)
2. Đăng nhập bằng tài khoản Google có quyền truy cập vào project BigQuery
3. Tạo báo cáo mới (Create > Report)
4. Chọn BigQuery làm nguồn dữ liệu
5. Kết nối với project, dataset và các view đã tạo

### Bước 3: Thiết kế Dashboard

#### Current Weather Dashboard

1. Thêm bộ lọc thành phố (Add a control > Drop-down list)
- Chọn nguồn dữ liệu: current_weather_view
- Dimension: city_name

2. Thêm scorecard hiển thị nhiệt độ hiện tại
- Chọn nguồn dữ liệu: current_weather_view
- Metric: temperature
- Filter: row_num = 1

3. Thêm gauge hiển thị độ ẩm và tốc độ gió
- Chọn nguồn dữ liệu: current_weather_view
- Metric: humidity/wind_speed
- Filter: row_num = 1
- Thiết lập thang đo phù hợp

4. Thêm time series chart cho nhiệt độ trong 24 giờ qua
- Chọn nguồn dữ liệu: current_weather_view
- Dimension: measurement_time
- Metric: temperature
- Thiết lập khoảng thời gian: Last 24 hours

#### Weather Trends Dashboard

1. Thêm bộ lọc thành phố và khoảng thời gian
- Chọn nguồn dữ liệu: weather_trends_view
- Dimension: city_name, date

2. Thêm line chart cho xu hướng nhiệt độ
- Chọn nguồn dữ liệu: weather_trends_view
- Dimension: date
- Metrics: avg_temp, max_temp, min_temp, temp_7day_moving_avg

3. Thêm line chart cho xu hướng độ ẩm, áp suất và tốc độ gió
- Chọn nguồn dữ liệu: weather_trends_view
- Dimension: date
- Metrics: avg_humidity/avg_pressure/avg_wind_speed

4. Thêm bảng so sánh giữa các thành phố
- Chọn nguồn dữ liệu: daily_weather_view
- Dimension: city_name
- Metrics: avg_temp, avg_humidity, avg_pressure, avg_wind_speed
- Filter: date = CURRENT_DATE() - 1

#### Weather Alerts Dashboard

1. Thêm bộ lọc thành phố và loại cảnh báo
- Chọn nguồn dữ liệu: weather_alerts_view
- Dimension: city_name, alert_type

2. Thêm scorecard hiển thị số lượng cảnh báo
- Chọn nguồn dữ liệu: weather_alerts_view
- Metric: Record Count

3. Thêm pie chart phân bố cảnh báo theo loại
- Chọn nguồn dữ liệu: weather_alerts_view
- Dimension: alert_type
- Metric: Record Count

4. Thêm bảng chi tiết các cảnh báo
- Chọn nguồn dữ liệu: weather_alerts_view
- Dimensions: city_name, alert_type, alert_level, temperature, humidity, wind_speed, measurement_time

#### Real-time Monitoring Dashboard

1. Thiết lập auto-refresh
- Report settings > Refresh every 5 minutes

2. Thêm sparkline cho nhiệt độ real-time
- Chọn nguồn dữ liệu: realtime_weather_view
- Dimension: window_start
- Metric: avg_temperature

3. Thêm gauge hiển thị các thông số hiện tại
- Chọn nguồn dữ liệu: realtime_weather_view
- Metrics: avg_temperature, avg_humidity, avg_pressure, avg_wind_speed
- Filter: window_start = MAX(window_start)

### Bước 4: Chia sẻ Dashboard

1. Click vào nút "Share" ở góc trên bên phải
2. Thiết lập quyền truy cập phù hợp
3. Sao chép link chia sẻ hoặc nhúng dashboard vào ứng dụng web
