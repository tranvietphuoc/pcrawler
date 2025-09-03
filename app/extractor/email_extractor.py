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
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        ]
        self.invalid_email = [
            r"^noreply@", 
            r"^no-reply@", 
            r"^example\.com$", 
            r"^test@", 
            r"^admin@", 
            r"^info@.*\.test$"
        ]
        
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
            
        base_timeout = self.config.processing_config.get("email_extraction_timeout", 45000)
        email_retry_delay = self.config.processing_config.get("email_retry_delay", [4, 10])
        
        timeout_strategy = [
            min(base_timeout * 0.4, 20000),    # 20s - nhanh nhất
            min(base_timeout * 0.6, 35000),    # 35s - trung bình
            min(base_timeout * 0.8, 50000),    # 50s - chậm hơn
            min(base_timeout * 1.0, 65000),    # 65s - chậm
            min(base_timeout * 1.2, 80000)     # 80s - chậm nhất
        ]
        
        for i in range(self.max_retries):
            try:
                current_timeout = timeout_strategy[i] / 1000
                print(f"[EmailExtractor] Attempt {i+1}/{self.max_retries} with timeout {current_timeout:.1f}s")
                
                res = await asyncio.wait_for(
                    self.crawler.arun(url=url, query=query),
                    timeout=current_timeout
                )
                
                content = (
                    getattr(res, "text", None)
                    or getattr(res, "content", None)
                    or str(res)
                )
                
                if not content or len(content.strip()) < 50:  # Giảm từ 100 xuống 50
                    print(f"[EmailExtractor] Content too short for {url}: {len(content or '')} chars")
                    continue
                
                emails = self.extract_emails_from_text(content)
                if emails:
                    print(f"[EmailExtractor] Successfully extracted {len(emails)} emails from {url}")
                    return emails
                else:
                    print(f"[EmailExtractor] No valid emails found in content from {url}")
                
                if i < self.max_retries - 1:
                    delay = random.uniform(
                        email_retry_delay[0] * (i + 1), 
                        min(email_retry_delay[1] * (i + 1), 15)  # Max 15s delay
                    )
                    print(f"[EmailExtractor] Retrying in {delay:.1f}s... (attempt {i+1}/{self.max_retries})")
                    await asyncio.sleep(delay)
                    
            except asyncio.TimeoutError:
                print(f"[EmailExtractor] Timeout for {url} (attempt {i+1}/{self.max_retries})")
                if i < self.max_retries - 1:
                    delay = random.uniform(
                        email_retry_delay[0] * (i + 1), 
                        min(email_retry_delay[1] * (i + 1), 15)
                    )
                    await asyncio.sleep(delay)
                continue
            except Exception as e:
                print(f"[EmailExtractor] Crawl attempt {i+1} failed for {url}: {e}")
                if i < self.max_retries - 1:
                    delay = random.uniform(
                        email_retry_delay[0] * (i + 1), 
                        min(email_retry_delay[1] * (i + 1), 15)
                    )
                    await asyncio.sleep(delay)
                continue
        
        print(f"[EmailExtractor] All attempts failed for {url}")
        return None

    async def _crawl_with_fallback(self, url: str, query: str):
        """Thử crawl với fallback strategies"""
        # Method 1: Progressive timeout
        result = await self._crawl(url, query)
        if result:
            return result
        
        try:
            print(f"[EmailExtractor] Trying quick fallback for {url}")
            result = await asyncio.wait_for(
                self.crawler.arun(url=url, query=query),
                timeout=25  #25s - nhanh nhưng đủ để lấy content
            )
            content = getattr(result, "text", None) or getattr(result, "content", None) or str(result)
            if content and len(content.strip()) > 50:  # Giảm requirement
                emails = self.extract_emails_from_text(content)
                if emails:
                    print(f"[EmailExtractor] Quick fallback successful: {len(emails)} emails")
                    return emails
        except Exception as e:
            print(f"[EmailExtractor] Quick fallback failed: {e}")
        
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
