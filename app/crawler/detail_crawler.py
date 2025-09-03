import asyncio, random, logging, gc
from typing import List, Dict, Any
from playwright.async_api import async_playwright
from config import CrawlerConfig
import psutil


logger = logging.getLogger(__name__)


class DetailCrawler:
    def __init__(
        self, config: CrawlerConfig = None, max_concurrent_pages: int = None, max_retries: int = None, delay_range=None
    ):
        self.config = config or CrawlerConfig()
        self.max_concurrent_pages = max_concurrent_pages or self.config.processing_config["max_concurrent_pages"]
        self.max_retries = max_retries or self.config.processing_config["max_retries"]
        self.delay_range = delay_range or self.config.processing_config["delay_range"]

    async def get_company_details(self, page, url: str) -> Dict[str, Any]:
        for attempt in range(self.max_retries):
            try:
                await asyncio.sleep(random.uniform(*self.delay_range))
                await page.goto(url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=self.config.processing_config["network_timeout"])
                
                # Wait for JavaScript to render dynamic content
                js_wait = self.config.processing_config.get("js_load_wait", 3000)
                # print(f"[JS] Waiting {js_wait}ms for JavaScript to render...")
                await asyncio.sleep(js_wait / 1000)

                def clean(x):
                    return x.strip() if (x and isinstance(x, str)) else "N/A"

                async def get_social(platform):
                    try:
                        link = await page.locator(
                            self.config.get_xpath("social_media_container", platform=platform)
                        ).get_attribute("href")
                        return clean(link) if link else "N/A"
                    except:
                        return "N/A"

                name = await page.locator(self.config.get_xpath("company_name")).text_content()
                address = await page.locator(self.config.get_xpath("company_address")).text_content()
                sub_address = await page.locator(self.config.get_xpath("company_address_fallback")).text_content()
                website = await page.locator(self.config.get_xpath("company_website")).get_attribute("href")
                phone = await page.locator(self.config.get_xpath("company_phone")).get_attribute("href")
                phone = phone.replace("tel:", "") if phone else "N/A"
                scale = await page.locator(self.config.get_xpath("company_scale")).text_content()
                created_year = await page.locator(self.config.get_xpath("company_created_year")).text_content()
                revenue = await page.locator(self.config.get_xpath("company_revenue")).text_content()
                return {
                    "name": clean(name),
                    "address": clean(address) if address else clean(sub_address),
                    "website": clean(website) if website else "N/A",
                    "phone": clean(phone),
                    "created_year": clean(created_year),
                    "revenue": clean(revenue),
                    "scale": clean(scale) if scale else "N/A",
                    "link": url,
                    "facebook": await get_social("facebook"),
                    "linkedin": await get_social("linkedin"),
                    "tiktok": await get_social("tiktok"),
                    "youtube": await get_social("youtube"),
                    "instagram": await get_social("instagram"),
                }
            except Exception:
                if attempt == self.max_retries - 1:
                    return {
                        "name": "N/A",
                        "address": "N/A",
                        "website": "N/A",
                        "phone": "N/A",
                        "created_year": "N/A",
                        "revenue": "N/A",
                        "scale": "N/A",
                        "link": url,
                        "facebook": "N/A",
                        "linkedin": "N/A",
                        "tiktok": "N/A",
                        "youtube": "N/A",
                        "instagram": "N/A",
                    }
                await asyncio.sleep(random.uniform(3, 7))

    async def crawl_company_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
        # Memory monitoring
        process = psutil.Process()
        mem_before = process.memory_info().rss // (1024 * 1024)
        print(f"[MEMORY][Batch] before: {mem_before} MB")

        # Random session delay to avoid detection
        session_delay = random.uniform(
            self.config.processing_config.get("session_delay", [12, 25])[0],
            self.config.processing_config.get("session_delay", [12, 25])[1]
        )
        print(f"[STEALTH] Session delay: {session_delay:.1f}s")
        await asyncio.sleep(session_delay)

        results = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-extensions",
                ],
            )
            
            # Random user agents to avoid detection
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            ]
            
            # Random viewport to avoid fingerprinting
            viewports = [
                {'width': 1280, 'height': 720},
                {'width': 1366, 'height': 768},
                {'width': 1440, 'height': 900},
                {'width': 1536, 'height': 864},
                {'width': 1920, 'height': 1080},
            ]
            
            context = await browser.new_context(
                viewport=random.choice(viewports),
                user_agent=random.choice(user_agents),
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                }
            )
            sem = asyncio.Semaphore(self.max_concurrent_pages)

            async def _one(u):
                async with sem:
                    page = await context.new_page()
                    try:
                        return await self.get_company_details(page, u)
                    finally:
                        await page.close()

            # Process in smaller chunks to avoid memory issues and reduce detection
            chunk_size = min(15, len(urls))  # Optimized chunk size for better data extraction
            for i in range(0, len(urls), chunk_size):
                chunk_urls = urls[i:i + chunk_size]
                batch = await asyncio.gather(
                    *[_one(u) for u in chunk_urls], return_exceptions=True
                )
                # Add random delay between chunks to avoid detection
                if i + chunk_size < len(urls):
                    batch_delay = random.uniform(
                        self.config.processing_config.get("batch_delay", [6, 12])[0],
                        self.config.processing_config.get("batch_delay", [6, 12])[1]
                    )
                    print(f"[STEALTH] Batch delay: {batch_delay:.1f}s")
                    await asyncio.sleep(batch_delay)
                for r in batch:
                    results.append(
                        r
                        if isinstance(r, dict)
                        else {
                            "name": "N/A",
                            "address": "N/A",
                            "website": "N/A",
                            "phone": "N/A",
                            "created_year": "N/A",
                            "revenue": "N/A",
                            "scale": "N/A",
                            "link": "N/A",
                            "facebook": "N/A",
                            "linkedin": "N/A",
                            "tiktok": "N/A",
                            "youtube": "N/A",
                            "instagram": "N/A",
                        }
                    )
            await context.close()
            await browser.close()
        # Memory cleanup
        gc.collect()
        mem_after = process.memory_info().rss // (1024 * 1024)
        print(f"[MEMORY][Batch] after GC: {mem_after} MB (freed ~{max(0, mem_before - mem_after)} MB)")
        return results
