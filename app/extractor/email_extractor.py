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
            
        self._initialized = True

    async def _crawl(self, url: str, query: str):
        """Simple crawl method theo document crawl4ai"""
        if not self.crawler or not url or url in ("N/A", ""):
            return None
            
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        # Simple timeout từ config
        timeout = self.config.processing_config.get("email_extraction_timeout", 45000) / 1000
        
        for attempt in range(self.max_retries):
            try:
                print(f"[EmailExtractor] Crawling {url} (attempt {attempt + 1}/{self.max_retries})")
                
                # Sử dụng crawl4ai theo document
                result = await asyncio.wait_for(
                    self.crawler.arun(url=url, query=query),
                    timeout=timeout
                )
                
                # Lấy content từ result
                content = getattr(result, "text", None) or getattr(result, "content", None) or str(result)
                
                if content and len(content.strip()) > 100:
                    # Extract emails từ content
                    emails = self._extract_emails(content)
                    if emails:
                        print(f"[EmailExtractor] Found {len(emails)} emails from {url}")
                        return emails
                    else:
                        print(f"[EmailExtractor] No emails found in content from {url}")
                else:
                    print(f"[EmailExtractor] Content too short from {url}: {len(content or '')} chars")
                
                # Delay trước khi retry
                if attempt < self.max_retries - 1:
                    delay = random.uniform(2, 5)
                    print(f"[EmailExtractor] Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    
            except asyncio.TimeoutError:
                print(f"[EmailExtractor] Timeout for {url}")
            except Exception as e:
                print(f"[EmailExtractor] Error crawling {url}: {e}")
                
                # Delay trước khi retry
                if attempt < self.max_retries - 1:
                    delay = random.uniform(2, 5)
                    await asyncio.sleep(delay)
        
        print(f"[EmailExtractor] Failed to extract emails from {url}")
        return None

    def _extract_emails(self, text: str) -> list:
        """Simple email extraction với regex cơ bản"""
        if not text:
            return []
        
        # Regex pattern đơn giản để tìm emails
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        
        # Loại bỏ emails không hợp lệ
        valid_emails = []
        for email in emails:
            email = email.strip().lower()
            # Loại bỏ emails rõ ràng không hợp lệ
            if (email.startswith('noreply@') or 
                email.startswith('no-reply@') or 
                email.endswith('@example.com') or
                email.endswith('@test.com')):
                continue
            valid_emails.append(email)
        
        # Trả về unique emails
        return list(set(valid_emails))

    async def from_website(self, website: str):
        """Extract emails từ website"""
        query = self.config.get_crawl4ai_query("website")
        return await self._crawl(website, query)

    async def from_facebook(self, fb: str):
        """Extract emails từ Facebook"""
        query = self.config.get_crawl4ai_query("facebook")
        return await self._crawl(fb, query)

    def cleanup(self):
        """Cleanup resources"""
        if hasattr(self, 'crawler') and self.crawler:
            try:
                if hasattr(self.crawler, 'close'):
                    try:
                        import asyncio
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.create_task(self.crawler.close())
                        else:
                            loop.run_until_complete(self.crawler.close())
                    except Exception as e:
                        print(f"[EmailExtractor] Async close failed: {e}")
                        del self.crawler
            except Exception as e:
                print(f"[EmailExtractor] Cleanup error: {e}")
            finally:
                self.crawler = None

    async def async_cleanup(self):
        """Async cleanup method"""
        if hasattr(self, 'crawler') and self.crawler:
            try:
                if hasattr(self.crawler, 'close'):
                    await self.crawler.close()
            except Exception as e:
                print(f"[EmailExtractor] Async cleanup error: {e}")
            finally:
                self.crawler = None
