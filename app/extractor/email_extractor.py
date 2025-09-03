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



    def _extract_emails(self, text: str) -> list:
        """Email extraction với xử lý obfuscation và regex linh hoạt"""
        if not text:
            return []
        
        # Chuẩn hóa các dạng obfuscation phổ biến
        s = text
        s = re.sub(r"\[(at|AT)\]|\(at\)|\s+at\s+", "@", s)
        s = re.sub(r"\[(dot|DOT)\]|\(dot\)|\s+dot\s+", ".", s)
        s = s.replace("[.]", ".").replace("(.)", ".")
        s = s.replace("[at]", "@").replace("(at)", "@")
        s = s.replace("[dot]", ".").replace("(dot)", ".")
        
        # Bắt cả mailto:
        s = s.replace("mailto:", "")
        
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
        emails = re.findall(email_pattern, s)
        
        blocked = (
            'noreply@', 'no-reply@', 'donotreply@', 'do-not-reply@',
            '@example.com', '@test.com', '@localhost',
        )
        out = []
        for email in emails:
            el = email.strip().lower()
            if el.startswith(blocked):
                continue
            if email not in out:
                out.append(email)
            if len(out) >= 3:
                break
        return out

    async def from_website(self, website: str):
        """Extract emails từ website thông thường"""
        if not website or website == "N/A":
            return None
            
        if not self.crawler:
            return None
            
        query = self.config.get_crawl4ai_query("website")
        
        try:
            result = await asyncio.wait_for(
                self.crawler.arun(url=website, query=query),
                timeout=45
            )
            
            content = getattr(result, "text", None) or getattr(result, "content", None) or str(result)
            emails = self._extract_emails(content)
            
            return emails[:3] if emails else None
            
        except Exception as e:
            print(f"[EmailExtractor] Website crawl failed: {e}")
            return None

    async def from_facebook(self, fb: str):
        """Extract emails từ Facebook với auto-close popup login"""
        if not fb or fb == "N/A":
            return None
            
        if not self.crawler:
            return None
            
        query = self.config.get_crawl4ai_query("facebook")
        
        try:
            # Sử dụng crawl4ai.arun trực tiếp với init_script
            result = await asyncio.wait_for(
                self.crawler.arun(
                    url=fb, 
                    query=query,
                    init_script="""
                        // Auto-close Facebook login popup
                        const closePopup = () => {
                            // Tìm và click nút X của popup
                            const closeBtn = document.querySelector('[aria-label="Close"], .x1n2onr6.x1ja2u2z, [data-testid="close-button"]');
                            if (closeBtn) {
                                closeBtn.click();
                                console.log('Closed Facebook login popup');
                            }
                            
                            // Hoặc tìm popup và remove
                            const popup = document.querySelector('[role="dialog"], .x1n2onr6.x1ja2u2z');
                            if (popup) {
                                popup.remove();
                                console.log('Removed Facebook popup');
                            }
                        };
                        
                        // Chạy ngay và sau 2s
                        closePopup();
                        setTimeout(closePopup, 2000);
                        
                        // Observer để đóng popup mới xuất hiện
                        const observer = new MutationObserver(() => {
                            closePopup();
                        });
                        observer.observe(document.body, { childList: true, subtree: true });
                    """
                ),
                timeout=45
            )
            
            content = getattr(result, "text", None) or getattr(result, "content", None) or str(result)
            emails = self._extract_emails(content)
            
            return emails[:3] if emails else None
            
        except Exception as e:
            print(f"[EmailExtractor] Facebook crawl failed: {e}")
            return None

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
