# app/tasks/celery_app.py
import os
from celery import Celery
from kombu import Queue

broker = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
backend = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery("crawler", broker=broker, backend=backend)

# (1) tell Celery where to find tasks
celery_app.conf.include = ["app.tasks.tasks"]
# or:
# celery_app.autodiscover_tasks(["app.tasks"])

# (2) QUEUE & PREFETCH (use -Q crawl)
celery_app.conf.task_default_queue = "crawl"
celery_app.conf.task_queues = (Queue("crawl"),)
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.task_routes = {
    "crawl.details_extract_write": {"queue": "crawl"},
    "merge.csv_files": {"queue": "crawl"},
}
