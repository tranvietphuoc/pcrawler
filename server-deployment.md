# PCrawler Server Deployment

## 🌐 **Truy cập từ bên ngoài:**

### **URLs để truy cập:**

- **N8N Dashboard:** http://117.79.202.225:5678
  - Username: `admin`
  - Password: `pcrawler123`
- **Flower (Celery Monitor):** http://117.79.202.225:5555

## 🚀 **Deploy trên Server:**

### **1. Upload files lên server:**

```bash
# Upload toàn bộ project lên server
scp -r /home/phuoc/workspace/python/crawl/pcrawler user@117.79.202.225:/opt/pcrawler/
```

### **2. SSH vào server:**

```bash
ssh user@117.79.202.225
cd /opt/pcrawler/
```

### **3. Deploy với Docker:**

```bash
# Stop containers cũ (nếu có)
docker-compose down

# Chạy với N8N
docker-compose -f docker-compose-n8n.yml up -d

# Xem logs
docker-compose -f docker-compose-n8n.yml logs -f
```

## 🔧 **Cấu hình Firewall:**

### **Mở ports cần thiết:**

```bash
# Ubuntu/Debian
sudo ufw allow 5678  # N8N
sudo ufw allow 5555  # Flower
sudo ufw allow 6379  # Redis (nếu cần)

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=5678/tcp
sudo firewall-cmd --permanent --add-port=5555/tcp
sudo firewall-cmd --reload
```

## 📊 **Monitoring từ xa:**

### **N8N Dashboard:**

1. Truy cập http://117.79.202.225:5678
2. Login: admin/pcrawler123
3. Xem execution history
4. Monitor workflow status
5. Retry failed executions

### **Flower Dashboard:**

1. Truy cập http://117.79.202.225:5555
2. Xem Celery tasks
3. Monitor worker health
4. Xem task queue status

### **Docker Logs:**

```bash
# SSH vào server
ssh user@117.79.202.225

# Xem logs tất cả services
docker-compose -f docker-compose-n8n.yml logs -f

# Xem logs specific service
docker-compose -f docker-compose-n8n.yml logs -f n8n
docker-compose -f docker-compose-n8n.yml logs -f pcrawler-app
```

## 🔄 **N8N Workflow Commands:**

### **Phase Commands (đã update cho Docker):**

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

## 🛠️ **Troubleshooting:**

### **Common Issues:**

1. **Không truy cập được:** Kiểm tra firewall và ports
2. **Container không start:** Kiểm tra logs
3. **Database locked:** Restart containers

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

- **Truy cập từ xa:** http://117.79.202.225:5678
- **Monitor real-time:** N8N + Flower dashboards
- **Không còn vòng lặp vô hạn**
- **Dễ dàng control từ bên ngoài**
- **Scalable và maintainable**
