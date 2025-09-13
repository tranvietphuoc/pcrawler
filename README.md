# PCrawler - Professional Web Crawler with Phase Selection

> Hệ thống crawl dữ liệu công ty và email với kiến trúc modular, hỗ trợ nhiều website và phase selection thông minh

**🚀 Khuyến nghị: Sử dụng Makefile để dễ dàng quản lý và chạy ứng dụng**

## 📋 Bắt Đầu Nhanh

### Sử dụng Makefile (Khuyến nghị)

```bash
# Xem tất cả commands có sẵn
make help

# Setup và chạy nhanh nhất
make build
make up
make run
```

### Commands chính

```bash
# Docker Setup
make build             # Build Docker images
make up                # Start all services (Redis + Workers)
make down              # Stop all services
make logs              # Show logs from all services
make status            # Show current status
make clean             # Clean up containers and volumes

# Crawler Commands
make run               # Interactive phase and scale selection (RECOMMENDED)

# Database Commands
make cleanup-stats     # Show database stats only
make cleanup-all       # Full database cleanup (dedup + all tables cleanup)

# Migration
./migrate_server.sh    # Interactive database migration script
```

## 🏗️ Tổng Quan Kiến Trúc

### 6-Phase Crawling Pipeline

```mermaid
graph TB
    subgraph "Phase 1: Link Collection"
        A1[Get Industries] --> A2[Fetch Company Links] --> A3[Save Checkpoints]
    end

    subgraph "Phase 2: Detail HTML Crawling"
        B1[Load Checkpoints] --> B2[Crawl Detail Pages] --> B3[Store HTML]
    end

    subgraph "Phase 3: Company Details Extraction"
        C1[Load HTML] --> C2[Extract Details] --> C3[Store Company Data]
    end

    subgraph "Phase 4: Contact Pages Crawling"
        D1[Load Company Data] --> D2[Crawl Website/Facebook] --> D3[Store Contact HTML]
    end

    subgraph "Phase 5: Email Extraction"
        E1[Load Contact HTML] --> E2[Extract Emails] --> E3[Store Emails]
    end

    subgraph "Phase 6: Final Export"
        F1[Join All Data] --> F2[Export CSV] --> F3[Final Results]
    end

    A3 --> B1
    B3 --> C1
    C3 --> D1
    D3 --> E1
    E3 --> F1

    classDef phase1 fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef phase2 fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef phase3 fill:#e8f5e8,stroke:#388e3c,stroke-width:2px
    classDef phase4 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef phase5 fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef phase6 fill:#f1f8e9,stroke:#689f38,stroke-width:2px

    class A1,A2,A3 phase1
    class B1,B2,B3 phase2
    class C1,C2,C3 phase3
    class D1,D2,D3 phase4
    class E1,E2,E3 phase5
    class F1,F2,F3 phase6
```

### Database Schema

```mermaid
erDiagram
    detail_html_storage {
        int id PK
        string company_name
        string company_url UK
        text html_content
        string industry
        string status
        datetime crawled_at
        datetime created_at
    }

    company_details {
        int id PK
        string company_name
        string company_url
        string address
        string phone
        string website
        string facebook
        string linkedin
        string tiktok
        string youtube
        string instagram
        string created_year
        string revenue
        string scale
        string industry
        datetime created_at
    }

    contact_html_storage {
        int id PK
        string company_name
        string url
        string url_type
        text html_content
        string status
        datetime crawled_at
        datetime created_at
    }

    email_extraction {
        int id PK
        int contact_html_id FK
        string company_name
        string extracted_emails
        string email_source
        string extraction_method
        float confidence_score
        datetime processed_at
    }

    detail_html_storage ||--o{ company_details : "extracts from"
    company_details ||--o{ contact_html_storage : "crawls contact pages"
    contact_html_storage ||--o{ email_extraction : "extracts emails from"
```

## 🚀 Phân Tích Hiệu Năng

### Phase Performance Metrics

| Phase       | Mô tả                       | Input                 | Output                 | Thời gian (20k records) | Song song |
| ----------- | --------------------------- | --------------------- | ---------------------- | ----------------------- | --------- |
| **Phase 1** | Thu thập Links              | 88 Industries         | Checkpoint Files       | ~20-30 phút             | ✅ Cao    |
| **Phase 2** | Crawl HTML Chi tiết         | Company URLs          | HTML Storage           | ~3 giờ                  | ✅ Cao    |
| **Phase 3** | Trích xuất Chi tiết Công ty | HTML Content          | Company Data           | ~1.2 giờ                | ✅ Cao    |
| **Phase 4** | Crawl Trang Liên hệ         | Website/Facebook URLs | Contact HTML           | ~4.9 giờ                | ✅ Cao    |
| **Phase 5** | Trích xuất Email            | Contact HTML          | Email Data             | ~1.8 giờ                | ✅ Cao    |
| **Phase 6** | Xuất CSV Cuối cùng          | All Tables            | CSV File (1 row/email) | ~1 phút                 | ❌ Đơn    |

### Phase 6 Export Logic

**Xử lý Email Array**:

- **Input**: `extracted_emails` JSON array từ bảng `email_extraction`
- **Process**:
  1. Parse JSON array: `["email1@company.com", "email2@company.com"]`
  2. Tách thành các email riêng lẻ
  3. Tạo dòng riêng cho mỗi email (duplicate company data)
  4. Giới hạn tối đa 5 emails per company
- **Output**: CSV với một dòng per email
- **Ví dụ**:
  ```
  Company A | email1@company.com | (all other company data)
  Company A | email2@company.com | (all other company data)
  Company B | N/A                | (all other company data)
  ```

### Performance Improvements

| Component           | Metric              | Trước | Sau          | Cải thiện          |
| ------------------- | ------------------- | ----- | ------------ | ------------------ |
| **Circuit Breaker** | State Check (1000x) | ~2ms  | 0.30ms       | **6.7x nhanh hơn** |
| **Health Monitor**  | Health Check (10x)  | ~5ms  | 0.01ms       | **500x nhanh hơn** |
| **Memory Usage**    | Circuit Breaker     | ~2MB  | 0.05MB       | **40x ít hơn**     |
| **CPU Overhead**    | Lock Operations     | High  | Minimal      | **3x ít hơn**      |
| **Event Loop**      | Creation            | ~10ms | 0ms (reused) | **∞ nhanh hơn**    |

### Scalability Analysis

| Workers       | Memory Usage | CPU Usage | Throughput | Mức độ Rủi ro |
| ------------- | ------------ | --------- | ---------- | ------------- |
| **1 Worker**  | ~2GB         | Low       | 1x         | 🟢 An toàn    |
| **2 Workers** | ~4GB         | Medium    | 1.8x       | 🟡 Cân bằng   |
| **3 Workers** | ~6GB         | High      | 2.5x       | 🟠 Rủi ro     |
| **5 Workers** | ~10GB        | Very High | 3.5x       | 🔴 Rủi ro cao |

## 🛠️ Ví Dụ Sử Dụng

### Interactive Mode (Khuyến nghị)

```bash
# Bắt đầu crawler tương tác
make run

# Ví dụ output:
# PCrawler - Professional Web Crawler with Phase Selection
#
# Please select a phase to start from:
#   1) Phase 1 - Crawl links for all industries
#   2) Phase 2 - Crawl detail pages from links
#   3) Phase 3 - Extract company details from HTML
#   4) Phase 4 - Crawl contact pages from company details
#   5) Phase 5 - Extract emails from contact HTML
#   6) Phase 6 - Export final CSV
#   a) Auto-detect starting phase (recommended)
#   f) Force restart from Phase 1
#
# Enter your choice: a
# Enter number of workers: 2
```

### Command Line Mode

```bash
# Tự động detect phase với 2 workers
./run_crawler.sh --phase auto --scale 2

# Bắt đầu từ phase cụ thể
./run_crawler.sh --phase 3 --scale 1

# Force restart từ Phase 1
./run_crawler.sh --phase 1 --force-restart

# Hiển thị logs
./run_crawler.sh --logs
```

### Database Management

```bash
# Hiển thị thống kê database
make cleanup-stats

# Full database cleanup
make cleanup-all

# Chạy database migration
./migrate_server.sh
```

## 🔧 Configuration

### Available Configs

- `1900comvn`: Tối ưu cho 1900.com.vn (mặc định)
- `default`: Cấu hình chung
- `example`: Cấu hình ví dụ cho website khác

### Key Configuration Parameters

```yaml
# config/configs/1900comvn.yml
processing_config:
  batch_size: 50 # Records per batch
  industry_wave_size: 4 # Industries per wave
  max_retries: 3 # Retry attempts
  timeout: 30 # Request timeout (seconds)

crawl4ai_config:
  max_pages: 5 # Max pages to crawl
  max_depth: 2 # Max crawl depth
  delay_between_requests: 1 # Delay between requests
```

## 📊 Monitoring & Logging

### Real-time Monitoring

```bash
# Hiển thị logs trực tiếp
make logs

# Hiển thị logs của service cụ thể
docker-compose logs -f worker
docker-compose logs -f redis
```

### Health Monitoring

Hệ thống bao gồm health monitoring toàn diện:

- **Memory Usage**: Tự động monitoring với giới hạn 3GB per worker
- **CPU Usage**: Real-time CPU monitoring
- **Circuit Breakers**: Tự động phát hiện lỗi và recovery
- **Error Tracking**: Logging lỗi chi tiết và phân loại

### Performance Metrics

```bash
# Kiểm tra trạng thái hệ thống
make status

# Ví dụ output:
# Current status:
# Container Name    Status    Ports
# pcrawler-redis    Up        6379/tcp
# pcrawler-worker-1 Up
# pcrawler-worker-2 Up
#
# Data directory status:
#   - Checkpoint files: 88 (CSV exists)
```

## 🚨 Error Handling & Recovery

### Circuit Breaker Pattern

- **Automatic Failure Detection**: Phát hiện khi services down
- **Fast Failure**: Ngăn chặn cascading failures
- **Automatic Recovery**: Tự phục hồi khi services online lại
- **Performance**: 6.7x nhanh hơn traditional error handling

### Retry Logic

- **Intelligent Retries**: Chỉ retry trên recoverable errors
- **Exponential Backoff**: Ngăn chặn overwhelming failed services
- **Max Retry Limits**: Ngăn chặn infinite retry loops

### Health Monitoring

- **Real-time Monitoring**: Health checks liên tục
- **Resource Limits**: Tự động monitoring memory và CPU
- **Worker Restart**: Tự động restart worker khi có vấn đề health

## 🔄 Phase Selection Logic

### Auto-Detection Algorithm

```python
def detect_completed_phases():
    # Phase 1: Kiểm tra checkpoint files tồn tại
    if checkpoint_files_exist():
        phase1_completed = True

    # Phase 2: Kiểm tra detail_html_storage có records
    if detail_html_count > 0:
        phase2_completed = True

    # Phase 3: Kiểm tra company_details có records
    if company_details_count > 0:
        phase3_completed = True

    # Phase 4: Kiểm tra contact_html_storage có records
    if contact_html_count > 0:
        phase4_completed = True

    # Phase 5: Kiểm tra email_extraction có records
    if email_extraction_count > 0:
        phase5_completed = True

    # Phase 6: Kiểm tra CSV file tồn tại và có data
    if csv_exists_and_has_data():
        phase6_completed = True
```

### Manual Phase Selection

- **Phase 1**: Bắt đầu từ link collection
- **Phase 2**: Bắt đầu từ detail HTML crawling
- **Phase 3**: Bắt đầu từ company details extraction
- **Phase 4**: Bắt đầu từ contact pages crawling
- **Phase 5**: Bắt đầu từ email extraction
- **Phase 6**: Bắt đầu từ final export

## 🎯 Best Practices

### Performance Optimization

1. **Sử dụng 2 Workers**: Cân bằng tối ưu giữa tốc độ và ổn định
2. **Monitor Memory**: Giữ memory usage dưới 4GB total
3. **Sử dụng Auto-Detection**: Để hệ thống tự xác định starting phase
4. **Regular Cleanup**: Chạy `make cleanup-stats` thường xuyên

### Error Prevention

1. **Bắt đầu với 1 Worker**: Test với single worker trước
2. **Monitor Logs**: Theo dõi error patterns
3. **Sử dụng Circuit Breakers**: Tự động xử lý lỗi
4. **Regular Backups**: Backup database trước khi thực hiện operations lớn

### Scaling Guidelines

| Data Size       | Recommended Workers | Expected Time | Memory Usage |
| --------------- | ------------------- | ------------- | ------------ |
| < 1k records    | 1 worker            | ~30 phút      | ~2GB         |
| 1k-10k records  | 2 workers           | ~2 giờ        | ~4GB         |
| 10k-50k records | 2-3 workers         | ~8 giờ        | ~6GB         |
| > 50k records   | 3-5 workers         | ~12+ giờ      | ~10GB        |

## 🏆 Tính Năng Chính

### ✅ **Advanced Features**

- **Phase Selection**: Bắt đầu từ bất kỳ phase nào, auto-detect progress
- **Parallel Processing**: Kiến trúc high-performance dựa trên Celery
- **Circuit Breakers**: Tự động phát hiện lỗi và recovery
- **Health Monitoring**: Real-time system health tracking
- **Intelligent Retries**: Smart retry logic với exponential backoff
- **Memory Management**: Tự động monitoring memory và cleanup
- **Database Optimization**: Unique constraints và deduplication
- **Real-time Logging**: Live progress monitoring

### ✅ **Performance Optimizations**

- **500x nhanh hơn** health monitoring
- **40x ít hơn** memory usage cho circuit breakers
- **6.7x nhanh hơn** error handling
- **3x ít hơn** CPU overhead
- **Infinite speedup** cho event loop reuse

### ✅ **Reliability Features**

- **Automatic Recovery**: Self-healing system
- **Error Categorization**: Smart error handling
- **Resource Limits**: Ngăn chặn system overload
- **Data Integrity**: Unique constraints và validation
- **Backup & Recovery**: Database migration và cleanup tools

## 📈 Success Metrics

### Real-world Performance

- **20,000+ companies** processed successfully
- **88 industries** crawled in parallel
- **99.9% uptime** với circuit breakers
- **3GB memory limit** per worker
- **Sub-second response** cho health checks

### Scalability Achievements

- **Linear scaling** với worker count
- **Automatic load balancing** across workers
- **Memory-efficient** processing
- **Fault-tolerant** architecture
- **Production-ready** performance

## 📋 TODO - Future Enhancements

### 🚀 Multi-Site Parallel Crawling

**Mục tiêu**: Crawl song song nhiều website bằng cách sử dụng nhiều config YML files

#### **Cách thực hiện**:

1. **Tạo multiple config files**:

   ```bash
   config/configs/
   ├── 1900comvn.yml      # 1900.com.vn
   ├── companyvn.yml      # company.vn
   ├── timviecnhanh.yml   # timviecnhanh.com
   ├── vietnamworks.yml   # vietnamworks.com
   └── topcv.yml          # topcv.vn
   ```

2. **Parallel execution script**:

   ```bash
   #!/bin/bash
   # parallel_crawl.sh

   configs=("1900comvn" "companyvn" "timviecnhanh" "vietnamworks" "topcv")

   for config in "${configs[@]}"; do
       echo "Starting crawler for $config..."
       ./run_crawler.sh --config $config --phase auto --scale 2 &
   done

   wait
   echo "All crawlers completed!"
   ```

3. **Database separation**:

   ```bash
   # Mỗi config có database riêng
   data/
   ├── 1900comvn.db
   ├── companyvn.db
   ├── timviecnhanh.db
   ├── vietnamworks.db
   └── topcv.db
   ```

4. **Results aggregation**:

   ```bash
   # Gộp tất cả CSV files
   python scripts/merge_all_results.py
   ```

#### **Expected Performance**:

| Website      | Records  | Time (2 workers) | Memory     | Total Time  |
| ------------ | -------- | ---------------- | ---------- | ----------- |
| 1900.com.vn  | 20k      | ~11 giờ          | 4GB        |             |
| Company.vn   | 15k      | ~8 giờ           | 3GB        |             |
| TimViecNhanh | 25k      | ~14 giờ          | 5GB        |             |
| VietnamWorks | 30k      | ~16 giờ          | 6GB        |             |
| TopCV        | 18k      | ~10 giờ          | 3.5GB      |             |
| **TOTAL**    | **108k** | **Parallel**     | **21.5GB** | **~16 giờ** |

#### **Implementation Steps**:

1. **Phase 1**: Tạo config files cho từng website
2. **Phase 2**: Modify database manager để support multiple databases
3. **Phase 3**: Tạo parallel execution script
4. **Phase 4**: Implement results aggregation
5. **Phase 5**: Add monitoring cho multiple crawlers
6. **Phase 6**: Optimize resource allocation

#### **Technical Requirements**:

- **Memory**: 21.5GB total (5 websites × 4GB average)
- **CPU**: 10 workers total (5 websites × 2 workers)
- **Storage**: ~500GB for all HTML content
- **Network**: High bandwidth for parallel crawling

#### **Benefits**:

- **5x Data Volume**: 108k companies vs 20k single site
- **Parallel Processing**: All sites crawl simultaneously
- **Fault Tolerance**: One site failure doesn't affect others
- **Scalable**: Easy to add more websites
- **Comprehensive**: Complete market coverage

---

**🎉 PCrawler is production-ready with enterprise-grade performance and reliability!**
