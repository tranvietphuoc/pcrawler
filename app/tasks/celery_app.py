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

# (2.5) HEARTBEAT CONFIGURATION - REDUCE WARNINGS
celery_app.conf.worker_heartbeat_interval = 30  # Increase heartbeat interval to 30s
celery_app.conf.worker_direct = True  # Direct worker communication
celery_app.conf.worker_send_task_events = False  # Disable task events to reduce overhead

# (3) RESULT BACKEND CONFIGURATION - ENABLE FOR PROPER RESULT HANDLING
celery_app.conf.result_expires = 3600  # Results expire after 1 hour
celery_app.conf.result_persistent = True  # Enable persistence
celery_app.conf.task_ignore_result = False  # Enable results for proper handling
celery_app.conf.task_store_eager_result = True  # Store eager results
celery_app.conf.result_compression = None  # Disable compression
celery_app.conf.result_serializer = 'json'  # Use JSON for security
celery_app.conf.accept_content = ['json']  # Only accept JSON
celery_app.conf.task_serializer = 'json'  # Use JSON for task serialization
celery_app.conf.result_backend_max_retries = 3  # Enable retries
celery_app.conf.result_backend_always_retry = True  # Always retry
celery_app.conf.result_backend_retry_delay = 1  # Minimal delay
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
