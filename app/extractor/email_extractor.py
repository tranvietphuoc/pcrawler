import asyncio, random, re
from config import CrawlerConfig

try:
    from crawl4ai import AsyncWebCrawler
except Exception:
    AsyncWebCrawler = None


class EmailExtractor:
    def __init__(self, config: CrawlerConfig = None, max_retries: int = None, delay_range=None):
        self.config = config or CrawlerConfig()
        self.max_retries = max_retries or self.config.processing_config["max_retries"]
        self.delay_range = delay_range or self.config.processing_config["delay_range"]
        self.crawler = AsyncWebCrawler() if AsyncWebCrawler else None
        self.email_patterns = [
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
            r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}",
        ]
        self.invalid_email = [r"noreply@", r"no-reply@", r"example\.com", r"@\d+\.\d+"]

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
            except Exception:
                if i < self.max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 4))
        return None

    async def from_website(self, website: str):
        query = self.config.get_crawl4ai_query("website")
        return await self._crawl(website, query)

    async def from_facebook(self, fb: str):
        query = self.config.get_crawl4ai_query("facebook")
        return await self._crawl(fb, query)
