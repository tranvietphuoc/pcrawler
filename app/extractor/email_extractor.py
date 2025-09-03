import asyncio, random, re
import os
from config import CrawlerConfig

try:
    from crawl4ai import AsyncWebCrawler
except Exception:
    AsyncWebCrawler = None

# Global instance per worker process
_worker_extractor = None

class EmailExtractor:
    def __new__(cls, *args, **kwargs):
        global _worker_extractor
        if _worker_extractor is None:
            _worker_extractor = super().__new__(cls)
        return _worker_extractor
    
    def __init__(self, config: CrawlerConfig = None, max_retries: int = None, delay_range=None):
        # Chỉ init một lần
        if hasattr(self, '_initialized'):
            return
            
        self.config = config or CrawlerConfig()
        self.max_retries = max_retries or self.config.processing_config["max_retries"]
        self.delay_range = delay_range or self.config.processing_config["delay_range"]
        
        # Set unique database path per worker để tránh lock
        worker_id = os.environ.get('CELERY_WORKER_ID', 'default')
        db_path = f"/tmp/crawl4ai_worker_{worker_id}.db"
        os.environ['CRAWL4AI_DB_PATH'] = db_path
        os.environ['CRAWL4AI_DB_TIMEOUT'] = '30000'  # 30 seconds timeout
        
        try:
            self.crawler = AsyncWebCrawler() if AsyncWebCrawler else None
            if self.crawler:
                print(f"[EmailExtractor] Initialized with database: {db_path}")
        except Exception as e:
            print(f"[EmailExtractor] Failed to initialize crawler: {e}")
            self.crawler = None
            
        self.email_patterns = [
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
            r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}",
        ]
        self.invalid_email = [r"noreply@", r"no-reply@", r"example\.com", r"@\d+\.\d+"]
        
        self._initialized = True

    def _find_emails(self, text):
        out = []
        for p in self.email_patterns:
            out += re.findall(p, text or "")
        return list({x.strip() for x in out if x})

    def _valid_email(self, e: str) -> bool:
        s = e.lower()
        for pat in self.invalid_email:
            if re.search(pat, s):
                return False
        return True

    def extract_emails_from_text(self, text: str):
        emails = [
            e for e in self._find_emails(text) if self._valid_email(e)
        ]
        return emails

    async def _crawl(self, url: str, query: str):
        if not self.crawler or not url or url in ("N/A", ""):
            return None
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
            
        for i in range(self.max_retries):
            try:
                res = await self.crawler.arun(url=url, query=query)
                content = (
                    getattr(res, "text", None)
                    or getattr(res, "content", None)
                    or str(res)
                )
                emails = self.extract_emails_from_text(content)
                if emails:
                    return emails
                await asyncio.sleep(random.uniform(*self.delay_range))
            except Exception as e:
                print(f"[EmailExtractor] Crawl attempt {i+1} failed for {url}: {e}")
                if i < self.max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 4))
        return None

    async def from_website(self, website: str):
        query = self.config.get_crawl4ai_query("website")
        return await self._crawl(website, query)

    async def from_facebook(self, fb: str):
        query = self.config.get_crawl4ai_query("facebook")
        return await self._crawl(fb, query)

    def cleanup(self):
        """Cleanup resources"""
        if hasattr(self, 'crawler') and self.crawler:
            try:
                # Close crawler if it has close method
                if hasattr(self.crawler, 'close'):
                    # AsyncWebCrawler.close() là coroutine, cần await
                    # Nhưng method này không async, nên dùng asyncio.create_task
                    try:
                        import asyncio
                        # Tạo task để chạy close() trong background
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # Nếu loop đang chạy, tạo task
                            asyncio.create_task(self.crawler.close())
                        else:
                            # Nếu loop không chạy, chạy trực tiếp
                            loop.run_until_complete(self.crawler.close())
                    except Exception as e:
                        print(f"[EmailExtractor] Async close failed: {e}")
                        # Fallback: xóa reference
                        del self.crawler
                elif hasattr(self.crawler, '__del__'):
                    del self.crawler
            except Exception as e:
                print(f"[EmailExtractor] Cleanup error: {e}")
            finally:
                self.crawler = None

    async def async_cleanup(self):
        """Async cleanup method - sử dụng khi có thể await"""
        if hasattr(self, 'crawler') and self.crawler:
            try:
                if hasattr(self.crawler, 'close'):
                    await self.crawler.close()
            except Exception as e:
                print(f"[EmailExtractor] Async cleanup error: {e}")
            finally:
                self.crawler = None
