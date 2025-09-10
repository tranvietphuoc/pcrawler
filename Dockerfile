FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libnss3 libnspr4 libdbus-1-3 \
    libatk1.0-0 libatk-bridge2.0-0 libasound2 \
    libx11-6 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 \
    libxkbcommon0 libxcb1 libgbm1 libgtk-3-0 \
    libcups2 libdrm2 libexpat1 libxshmfence1 \
    libpangocairo-1.0-0 libpango-1.0-0 libfontconfig1 \
    ca-certificates fonts-liberation libu2f-udev libvulkan1 wget gnupg \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r crawler && useradd -r -g crawler crawler

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && python -m playwright install chromium && python -m playwright install-deps chromium
COPY . .

# Change ownership to non-root user
RUN chown -R crawler:crawler /app

# Switch to non-root user
USER crawler

ENV PYTHONUNBUFFERED=1 TZ=Asia/Ho_Chi_Minh \
    PYTHONPATH=/app \
    CELERY_BROKER_URL=redis://redis:6379/0 \
    CELERY_RESULT_BACKEND=redis://redis:6379/0
CMD ["bash"]
