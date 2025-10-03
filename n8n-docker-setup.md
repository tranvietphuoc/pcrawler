# N8N + PCrawler Docker Setup

## 🚀 **Deploy với Docker Compose:**

### **1. Chạy N8N + PCrawler:**

```bash
# Stop containers hiện tại
docker-compose down

# Chạy với N8N
docker-compose -f docker-compose-n8n.yml up -d

# Xem logs
docker-compose -f docker-compose-n8n.yml logs -f
```

### **2. Truy cập các services:**

- **N8N Dashboard:** http://localhost:5678
  - Username: `admin`
  - Password: `pcrawler123`
- **Flower (Celery Monitor):** http://localhost:5555
- **PCrawler App:** Container `pcrawler-app`

### **3. Import N8N Workflow:**

1. Truy cập http://localhost:5678
2. Login với admin/pcrawler123
3. Import file `n8n_workflow.json`
4. Activate workflow

## 🔧 **N8N Workflow Commands:**

### **Phase Commands trong N8N:**

```bash
# Phase 1: Crawl Links
docker exec pcrawler-app python app/main.py crawl --config 1900comvn --phase 1

# Phase 2: Crawl Details
docker exec pcrawler-app python app/main.py crawl --config 1900comvn --phase 2

# Phase 3: Extract Details
docker exec pcrawler-app python app/main.py crawl --config 1900comvn --phase 3

# Phase 4: Crawl Contacts
docker exec pcrawler-app python app/main.py crawl --config 1900comvn --phase 4

# Phase 5: Extract Emails
docker exec pcrawler-app python app/main.py crawl --config 1900comvn --phase 5

# Phase 6: Export CSV
docker exec pcrawler-app python app/main.py crawl --config 1900comvn --phase 6
```

### **Database Commands:**

```bash
# Cleanup duplicates
docker exec pcrawler-app python cleanup_duplicates.py --cleanup-all-tables

# Migrate database
docker exec pcrawler-app python migrate_contact_unique.py

# Check database status
docker exec pcrawler-app sqlite3 data/crawler.db "SELECT status, COUNT(*) FROM detail_html_storage GROUP BY status;"
```

## 📊 **Monitoring:**

### **N8N Dashboard:**

- Xem execution history
- Xem logs từng phase
- Retry failed executions
- Manual trigger workflows

### **Flower Dashboard:**

- Monitor Celery tasks
- Xem task queue status
- Worker health monitoring

### **Docker Logs:**

```bash
# Xem logs tất cả services
docker-compose -f docker-compose-n8n.yml logs -f

# Xem logs specific service
docker-compose -f docker-compose-n8n.yml logs -f n8n
docker-compose -f docker-compose-n8n.yml logs -f pcrawler-app
```

## 🔄 **Workflow Logic:**

### **N8N Workflow:**

```
Cron Trigger → Cleanup DB → Migrate DB → Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6
```

### **Error Handling:**

- Mỗi phase có IF condition kiểm tra exit code
- Nếu fail → gửi alert và dừng
- Nếu success → tiếp tục phase tiếp theo

### **Manual Control:**

- Có thể chạy từng phase riêng biệt
- Có thể skip phases đã hoàn thành
- Có thể restart từ phase bất kỳ

## 🛠️ **Troubleshooting:**

### **Common Issues:**

1. **Container không start:** Kiểm tra logs
2. **Database locked:** Restart containers
3. **Permission denied:** Chạy `chmod +x` cho scripts

### **Debug Commands:**

```bash
# Vào container
docker exec -it pcrawler-app bash

# Check database
docker exec pcrawler-app sqlite3 data/crawler.db ".tables"

# Check Celery status
docker exec pcrawler-app celery -A app.tasks.celery_app inspect active

# Restart specific service
docker-compose -f docker-compose-n8n.yml restart pcrawler-app
```

## 🎯 **Kết quả:**

- **Không còn vòng lặp vô hạn**
- **Dễ dàng monitor từ N8N dashboard**
- **Tự động retry và error handling**
- **Scalable với Docker**
- **Maintainable với N8N workflow**
