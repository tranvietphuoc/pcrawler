# Circuit Breaker và Health Monitor Integration

## 📋 Tổng Quan

Hệ thống crawler đã được tích hợp **Circuit Breaker** và **Health Monitor** để cải thiện độ tin cậy và khả năng phục hồi:

### 🔧 Circuit Breaker

- **Mục đích**: Ngăn chặn cascading failures, fail fast khi có lỗi
- **Hoạt động**: Tự động mở circuit khi có quá nhiều lỗi, đóng lại sau thời gian recovery
- **Tích hợp**: Bảo vệ các operations chính như fetch links và crawl details

### 🏥 Health Monitor

- **Mục đích**: Theo dõi tình trạng worker (memory, CPU, browser count)
- **Hoạt động**: Kiểm tra health định kỳ, tự động cleanup khi cần
- **Tích hợp**: Monitor trước và sau mỗi batch operation

## 🚀 Cách Sử Dụng

### 1. Chạy Crawler với Circuit Breaker và Health Monitor

```bash
# Start Celery workers
docker-compose up -d worker

# Start crawler app
docker-compose up crawler_app
```

### 2. Monitor Health Định Kỳ

```bash
# Chạy health monitor daemon
python health_monitor_daemon.py
```

### 3. Test Integration

```bash
# Test circuit breaker và health monitor
python test_circuit_breaker_health_monitor.py

# Test Celery integration
python test_celery_integration.py
```

## 📊 Cấu Hình

### Circuit Breaker Settings

```python
# Trong tasks.py
breaker = circuit_manager.get_breaker(
    name=f"industry_links_{industry_id}",
    failure_threshold=3,        # Số lỗi tối đa trước khi mở circuit
    recovery_timeout=120,       # Thời gian recovery (giây)
    expected_exception=Exception
)
```

### Health Monitor Settings

```python
# Trong health_monitor.py
class HealthMonitor:
    def __init__(self):
        self.memory_threshold_mb = 1000      # Ngưỡng memory (MB)
        self.cpu_threshold_percent = 80      # Ngưỡng CPU (%)
        self.browser_threshold = 10          # Ngưỡng số browser
```

## 🔍 Monitoring

### Health Check Results

```
[2025-09-10 09:30:00] Health Check Results:
   ✅ Health Status: healthy
   💾 Memory Usage: 512.3MB
   🖥️  CPU Usage: 45.2%
   🔧 Active Tasks: 3
   🌐 Browser Count: 2
   📄 Context Count: 4
   🔌 Circuit Breakers:
      🟢 industry_links_1: CLOSED (failures: 0)
      🟢 detail_crawling: CLOSED (failures: 0)
```

### Circuit Breaker States

- **🟢 CLOSED**: Hoạt động bình thường
- **🔴 OPEN**: Circuit mở, fail fast
- **🟡 HALF_OPEN**: Đang test recovery

## 🛠️ Troubleshooting

### 1. Circuit Breaker Mở

```bash
# Kiểm tra circuit breaker states
python -c "
from app.utils.circuit_breaker import circuit_manager
import asyncio
async def check():
    states = await circuit_manager.get_all_states()
    print(states)
asyncio.run(check())
"
```

### 2. Health Issues

```bash
# Kiểm tra health summary
python -c "
from app.utils.health_monitor import health_monitor
print(health_monitor.get_health_summary())
"
```

### 3. Reset Circuit Breaker

```bash
# Reset tất cả circuit breakers
python -c "
from app.utils.circuit_breaker import circuit_manager
circuit_manager.reset_breaker('industry_links_1')
circuit_manager.reset_breaker('detail_crawling')
print('Circuit breakers reset')
"
```

## 📈 Performance Benefits

### Trước Khi Có Circuit Breaker

- ❌ Cascading failures
- ❌ Resource waste
- ❌ Long recovery time
- ❌ No health monitoring

### Sau Khi Có Circuit Breaker

- ✅ Fail fast protection
- ✅ Resource conservation
- ✅ Faster recovery
- ✅ Proactive health monitoring
- ✅ Automatic cleanup

## 🔧 Customization

### Thay Đổi Thresholds

```python
# Trong tasks.py - điều chỉnh failure threshold
breaker = circuit_manager.get_breaker(
    name="custom_breaker",
    failure_threshold=5,        # Tăng từ 3 lên 5
    recovery_timeout=300,       # Tăng từ 120s lên 300s
)
```

### Thay Đổi Health Check Interval

```python
# Trong health_monitor_daemon.py
daemon = HealthMonitorDaemon(check_interval=30)  # Check every 30 seconds
```

## 📝 Logs

### Circuit Breaker Logs

```
2025-09-10 09:30:00 - INFO - CircuitBreaker 'industry_links_1' initialized: threshold=3, timeout=120s
2025-09-10 09:30:05 - WARNING - CircuitBreaker 'industry_links_1' failure count: 1/3
2025-09-10 09:30:10 - ERROR - CircuitBreaker 'industry_links_1' is now OPEN - failing fast for 120s
```

### Health Monitor Logs

```
2025-09-10 09:30:00 - INFO - HealthMonitor initialized for worker_12345
2025-09-10 09:30:00 - INFO - Worker health OK: 512.3MB, 45.2% CPU
2025-09-10 09:30:05 - WARNING - Worker health issues detected: ['high_memory_usage']
```

## 🎯 Best Practices

1. **Monitor Regularly**: Chạy health monitor daemon
2. **Adjust Thresholds**: Điều chỉnh theo workload
3. **Check Logs**: Theo dõi circuit breaker và health logs
4. **Reset When Needed**: Reset circuit breakers khi cần
5. **Scale Workers**: Tăng số workers khi circuit breakers mở nhiều

## 🚨 Alerts

### Khi Nào Cần Chú Ý

- Circuit breaker mở > 5 phút
- Memory usage > 1GB
- CPU usage > 80%
- Browser count > 10
- Health check failed > 3 lần liên tiếp

### Hành Động Khuyến Nghị

1. **Circuit Breaker Mở**: Kiểm tra target website, tăng timeout
2. **High Memory**: Restart workers, giảm batch size
3. **High CPU**: Giảm concurrency, tăng delay
4. **Health Check Failed**: Restart workers, kiểm tra logs
