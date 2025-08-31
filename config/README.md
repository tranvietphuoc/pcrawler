# Config Directory

Thư mục này chứa hệ thống cấu hình YAML cho crawler.

## Cấu trúc

```
config/
├── __init__.py           # Import config classes
├── crawler_config.py     # CrawlerConfig class - load và quản lý YAML files
├── configs/              # Thư mục chứa các file YAML config
│   ├── default.yml       # Config mặc định
│   ├── 1900comvn.yml     # Config cho 1900.com.vn
│   └── example.yml       # Config ví dụ cho website khác
└── README.md             # File này
```

## Sử dụng

```python
from config import CrawlerConfig

# Load config
config = CrawlerConfig("1900comvn")

# Sử dụng config
base_url = config.website_config["base_url"]
xpath = config.get_xpath("company_name")
```

## Quản lý config

```bash
# Liệt kê config có sẵn
ls configs/

# Xem nội dung config
cat configs/1900comvn.yml

# Tạo config mới
cp configs/default.yml configs/mywebsite.yml
# Sau đó edit file mywebsite.yml
```
