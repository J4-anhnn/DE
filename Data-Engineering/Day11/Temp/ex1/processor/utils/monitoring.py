import matplotlib.pyplot as plt
import time
import threading
from collections import deque
import os

class StreamingMonitor:
    def __init__(self, window_size=60):
        self.window_size = window_size
        self.message_counts = deque(maxlen=window_size)
        self.processing_times = deque(maxlen=window_size)
        self.timestamps = deque(maxlen=window_size)
        self.is_running = False
        self.lock = threading.Lock()
    
    def record_message(self, processing_time_ms):
        """Ghi nhận một message đã được xử lý"""
        with self.lock:
            current_time = time.time()
            self.message_counts.append(1)
            self.processing_times.append(processing_time_ms)
            self.timestamps.append(current_time)
    
    def start_monitoring(self):
        """Bắt đầu một thread giám sát riêng"""
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print("Đã bắt đầu giám sát processing metrics")
    
    def stop_monitoring(self):
        """Dừng thread giám sát"""
        self.is_running = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=2.0)
        print("Đã dừng giám sát")
        
    def _monitor_loop(self):
        """Loop chính để giám sát và in metrics"""
        last_report_time = time.time()
        while self.is_running:
            current_time = time.time()
            
            # Báo cáo mỗi 10 giây
            if current_time - last_report_time >= 10.0:
                self._report_metrics()
                last_report_time = current_time
                
            # Ngủ một chút để tránh tiêu tốn CPU
            time.sleep(1.0)
    
    def _report_metrics(self):
        """Báo cáo các metrics hiện tại"""
        with self.lock:
            if not self.timestamps:
                print("Chưa có dữ liệu để báo cáo")
                return
                
            # Tính toán tốc độ xử lý message
            now = time.time()
            cutoff_time = now - 60  # xem xét 1 phút trước
            recent_count = sum(1 for t in self.timestamps if t >= cutoff_time)
            
            # Tính thời gian xử lý trung bình
            if self.processing_times:
                avg_proc_time = sum(self.processing_times) / len(self.processing_times)
            else:
                avg_proc_time = 0
            
            print(f"--- Báo cáo Metrics ---")
            print(f"Messages/phút: {recent_count}")
            print(f"Thời gian xử lý trung bình: {avg_proc_time:.2f} ms")
            print("----------------------")
    
    def generate_chart(self, output_file="processing_metrics.png"):
        """Tạo biểu đồ từ metrics đã thu thập"""
        with self.lock:
            if not self.timestamps or not self.processing_times:
                print("Không đủ dữ liệu để tạo biểu đồ")
                return
                
            # Chuyển đổi thành mảng tương đối thời gian
            rel_times = [t - self.timestamps[0] for t in self.timestamps]
            
            plt.figure(figsize=(12, 6))
            
            # Biểu đồ thời gian xử lý
            plt.subplot(1, 2, 1)
            plt.plot(rel_times, self.processing_times, 'b-')
            plt.title('Thời gian xử lý message')
            plt.xlabel('Thời gian (s)')
            plt.ylabel('Thời gian xử lý (ms)')
            plt.grid(True)
            
            # Biểu đồ tốc độ xử lý
            plt.subplot(1, 2, 2)
            # Tính message/s cho mỗi khoảng thời gian 5 giây
            window_size = 5.0  # seconds
            windows = []
            counts = []
            
            for i in range(0, int(max(rel_times)) + 1, int(window_size)):
                window_start = i
                window_end = i + window_size
                count = sum(1 for t in rel_times if window_start <= t < window_end)
                if count > 0:
                    windows.append(i)
                    counts.append(count / window_size)  # messages/second
            
            plt.bar(windows, counts, width=window_size*0.8)
            plt.title('Tốc độ xử lý message')
            plt.xlabel('Thời gian (s)')
            plt.ylabel('Messages/second')
            plt.grid(True)
            
            plt.tight_layout()
            plt.savefig(output_file)
            print(f"Đã lưu biểu đồ tại {output_file}")

if __name__ == "__main__":
    # Test monitor
    monitor = StreamingMonitor()
    monitor.start_monitoring()
    
    # Giả lập việc xử lý messages
    try:
        for i in range(100):
            # Giả lập thời gian xử lý ngẫu nhiên
            proc_time = 100 + (i % 10) * 20
            monitor.record_message(proc_time)
            print(f"Đã xử lý message {i+1}, thời gian: {proc_time} ms")
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        monitor.stop_monitoring()
        monitor.generate_chart()
