import asyncio, random, re
import os
from config import CrawlerConfig

try:
    from crawl4ai import AsyncWebCrawler
except Exception:
    AsyncWebCrawler = None

# Thử import AdaptiveCrawler
try:
    from crawl4ai import AdaptiveCrawler, AdaptiveConfig
except Exception:
    AdaptiveCrawler = None
    AdaptiveConfig = None

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
        
        # Adaptive flags
        pcfg = self.config.processing_config
        self.use_adaptive = bool(pcfg.get("email_adaptive", False))
        self.adaptive_conf = {
            "confidence_threshold": float(pcfg.get("email_adaptive_confidence_threshold", 0.75)),
            "max_pages": int(pcfg.get("email_adaptive_max_pages", 10)),
            "top_k_links": int(pcfg.get("email_adaptive_top_k_links", 4)),
        }
        
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

    async def _crawl_adaptive(self, url: str, query: str):
        if not self.crawler or not AdaptiveCrawler or not AdaptiveConfig:
            return None
        try:
            cfg = AdaptiveConfig(
                strategy="statistical",
                confidence_threshold=self.adaptive_conf["confidence_threshold"],
                max_pages=self.adaptive_conf["max_pages"],
                top_k_links=self.adaptive_conf["top_k_links"],
            )
            adaptive = AdaptiveCrawler(self.crawler, cfg)
            await adaptive.digest(start_url=url, query=query)
            docs = adaptive.get_relevant_content(top_k=5)
            text = "\n".join(d.get("content", "") for d in docs if d.get("content"))
            emails = self._extract_emails(text)
            if emails:
                print(f"[EmailExtractor] (Adaptive) Found {len(emails)} emails from {url}")
                return emails[:3]
        except Exception as e:
            print(f"[EmailExtractor] Adaptive mode failed for {url}: {e}")
        return None

    async def _crawl(self, url: str, query: str):
        """Simple crawl method theo document crawl4ai"""
        if not self.crawler or not url or url in ("N/A", ""):
            return None
        # Không chuẩn hóa scheme theo yêu cầu: dùng URL nguyên bản
        
        # Nếu bật adaptive thì thử trước
        if self.use_adaptive:
            result = await self._crawl_adaptive(url, query)
            if result:
                return result
        
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
                raw = (content or "").strip()
                # Parse trực tiếp output: cho phép nhiều delimiter ; , | \n
                parts = [p.strip() for p in re.split(r"[;|,\n]+", raw) if p and p.strip()]
                candidates = parts if parts else [raw] if raw else []

                # Validate nâng cao: TLD >= 2, loại email pixel/tracking, giới hạn 3
                emails = []
                email_regex = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
                block_keywords = (
                    'noreply@', 'no-reply@', 'donotreply@', 'do-not-reply@',
                    '@example.com', '@test.com', '@localhost',
                )
                block_domain_parts = (
                    'pixel', 'tracker', 'analytics', 'doubleclick', 'adservice', 'ads', 'utm', 'ga-'
                )
                for e in candidates:
                    e = e.replace("mailto:", "").strip()
                    if not email_regex.search(e):
                        continue
                    el = e.lower()
                    if el.startswith(block_keywords) or any(part in el.split('@')[-1] for part in block_domain_parts):
                        continue
                    if e not in emails:
                        emails.append(e)
                    if len(emails) >= 3:
                        break
                
                if emails:
                    print(f"[EmailExtractor] Found {len(emails)} emails from {url}")
                    return emails
                else:
                    print(f"[EmailExtractor] No emails parsed from AI output for {url}")
                
                # Delay trước khi retry
                if attempt < self.max_retries - 1:
                    delay = random.uniform(2, 6)
                    print(f"[EmailExtractor] Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    
            except asyncio.TimeoutError:
                print(f"[EmailExtractor] Timeout for {url}")
            except Exception as e:
                print(f"[EmailExtractor] Error crawling {url}: {e}")
                
                # Delay trước khi retry
                if attempt < self.max_retries - 1:
                    delay = random.uniform(2, 6)
                    await asyncio.sleep(delay)
        
        print(f"[EmailExtractor] Failed to extract emails from {url}")
        return None

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
