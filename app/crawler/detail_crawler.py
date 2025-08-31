import asyncio, random, logging
from typing import List, Dict, Any
from playwright.async_api import async_playwright
from config import CrawlerConfig


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
                await asyncio.sleep(random.uniform(2, 5))

    async def crawl_company_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
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
                ],
            )
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            sem = asyncio.Semaphore(self.max_concurrent_pages)

            async def _one(u):
                async with sem:
                    page = await context.new_page()
                    try:
                        return await self.get_company_details(page, u)
                    finally:
                        await page.close()

            # Process in smaller chunks to avoid memory issues
            chunk_size = min(20, len(urls))
            for i in range(0, len(urls), chunk_size):
                chunk_urls = urls[i:i + chunk_size]
                batch = await asyncio.gather(
                    *[_one(u) for u in chunk_urls], return_exceptions=True
                )
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
        return results
