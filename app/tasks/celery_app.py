# app/tasks/celery_app.py
import os
from celery import Celery
from kombu import Queue

broker = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
backend = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery("crawler", broker=broker, backend=backend)

# (1) tell Celery where to find tasks
# Use new phased tasks
celery_app.conf.include = ["app.tasks.tasks"]
# or:
# celery_app.autodiscover_tasks(["app.tasks"])

# (2) QUEUE & PREFETCH (use -Q crawl)
celery_app.conf.task_default_queue = "crawl"
celery_app.conf.task_queues = (Queue("crawl"),)
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.task_acks_late = True  # Only ack after task completion
celery_app.conf.worker_disable_rate_limits = True  # Disable rate limits
celery_app.conf.task_reject_on_worker_lost = True  # Reject tasks if worker lost
celery_app.conf.task_acks_on_failure_or_timeout = True  # Ack failed/timeout tasks

# (3) RESULT BACKEND CONFIGURATION - FIX EXCEPTION SERIALIZATION
celery_app.conf.result_expires = 1800  # Results expire after 30 minutes
celery_app.conf.result_persistent = False  # Disable persistence to avoid corruption
celery_app.conf.task_ignore_result = False  # Don't ignore results
celery_app.conf.task_store_eager_result = False  # Don't store eager results
celery_app.conf.result_compression = None  # Disable compression to avoid issues
celery_app.conf.result_serializer = 'pickle'  # Use pickle for better exception handling
celery_app.conf.accept_content = ['pickle', 'json']  # Accept both formats
celery_app.conf.task_serializer = 'pickle'  # Use pickle for task serialization
celery_app.conf.result_backend_max_retries = 1  # Reduce retries
celery_app.conf.result_backend_always_retry = False  # Don't always retry
celery_app.conf.result_backend_retry_delay = 2  # Increase delay
celery_app.conf.result_backend_retry_jitter = False  # Disable jitter
celery_app.conf.task_routes = {
    # Phase 0: Link fetching
    "links.fetch_industry_links": {"queue": "crawl"},
    # Phase 1: Detail crawling
    "detail.crawl_and_store": {"queue": "crawl"},
    # Phase 2: Detail extraction
    "detail.extract_from_html": {"queue": "crawl"},
    # Phase 3: Contact crawling
    "contact.crawl_from_details": {"queue": "crawl"},
    # Phase 4: Email extraction
    "email.extract_from_contact": {"queue": "crawl"},
    # Phase 5: Database operations
    "db.create_final_results": {"queue": "crawl"},
    "db.get_stats": {"queue": "crawl"},
    # Phase 6: Export
    "final.export": {"queue": "crawl"},
}

# Celery sẽ tự quản lý event loop với asyncio support
