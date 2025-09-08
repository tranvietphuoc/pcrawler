# app/tasks/celery_app.py
import os
from celery import Celery
from kombu import Queue

broker = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
backend = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery("crawler", broker=broker, backend=backend)

# (1) tell Celery where to find tasks
# Use new phased tasks
celery_app.conf.include = ["app.tasks.html_tasks"]
# or:
# celery_app.autodiscover_tasks(["app.tasks"])

# (2) QUEUE & PREFETCH (use -Q crawl)
celery_app.conf.task_default_queue = "crawl"
celery_app.conf.task_queues = (Queue("crawl"),)
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.task_routes = {
    # Phase 1
    "html.crawl_detail_pages": {"queue": "crawl"},
    # Phase 2
    "detail.extract_from_html": {"queue": "crawl"},
    # Phase 3
    "contact.crawl_from_details": {"queue": "crawl"},
    # Phase 4
    "email.extract_from_contact": {"queue": "crawl"},
    # Phase 5
    "db.create_final_results": {"queue": "crawl"},
    # Stats
    "db.get_stats": {"queue": "crawl"},
}

# Celery sẽ tự quản lý event loop với asyncio support
