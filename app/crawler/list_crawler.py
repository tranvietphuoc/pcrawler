import re, asyncio, random, logging
from typing import List, Tuple
from playwright.async_api import async_playwright
from config import CrawlerConfig

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ListCrawler:
    def __init__(self, config: CrawlerConfig = None, max_retries: int = None, delay_range=None):
        self.config = config or CrawlerConfig()
        self.max_retries = max_retries or self.config.processing_config["max_retries"]
        self.delay_range = delay_range or self.config.processing_config["delay_range"]

    async def _open_context(self):
        p = await async_playwright().start()
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context()
        page = await context.new_page()
        return p, browser, context, page

    async def get_total_pages(self, page, url: str) -> int:
        for i in range(self.max_retries):
            try:
                await page.goto(url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=self.config.processing_config["network_timeout"])
                links = await page.locator(self.config.get_xpath("pagination_links")).all()
                if links:
                    href = await links[-2].get_attribute("href")
                    m = re.search(r"\d+$", href or "")
                    if m:
                        return int(m.group())
                return 1
            except Exception:
                if i == self.max_retries - 1:
                    return 1
                await asyncio.sleep(random.uniform(2, 5))

    async def _get_total_pages_current(self, page) -> int:
        try:
            await page.wait_for_selector(self.config.get_xpath("pagination_links"), timeout=5000)
        except Exception:
            return 1
        links = await page.locator(self.config.get_xpath("pagination_links")).all()
        if not links:
            return 1
        try:
            href = await links[-2].get_attribute("href")
            m = re.search(r"\d+$", href or "")
            return int(m.group()) if m else 1
        except Exception:
            return 1

    async def get_industries(self, base_url: str) -> List[Tuple[str, str]]:
        p, browser, context, page = await self._open_context()
        try:
            await page.goto(base_url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=self.config.processing_config["network_timeout"])

            # 1) Select2 của trường "Ngành"
            select2_selection = page.locator(self.config.get_xpath("industry_select"))
            if await select2_selection.count() == 0:
                select2_selection = page.locator(self.config.get_xpath("industry_select_fallback"))

            # 2) Mở dropdown bằng 'mousedown'
            await select2_selection.first.dispatch_event("mousedown")

            # 3) Panel + ô search
            results_panel = page.locator(self.config.get_xpath("industry_results"))
            await results_panel.wait_for(state="visible", timeout=30000)

            search_input = page.locator(self.config.get_xpath("industry_search"))
            if await search_input.count():
                await search_input.first.fill(" ")
                await asyncio.sleep(0.4)

            # 4) Scroll load hết option
            prev = -1
            for _ in range(80):
                count = await page.locator(self.config.get_xpath("industry_options")).count()
                if count == prev:
                    break
                prev = count
                await page.evaluate(
                    "(sel)=>{const el=document.querySelector(sel); if(el){el.scrollTop=el.scrollHeight;}}",
                    "ul.select2-results__options",
                )
                await asyncio.sleep(0.25)

            # 5) Thu thập (value, text)
            nodes = await page.locator(self.config.get_xpath("industry_options")).all()

            out: List[Tuple[str, str]] = []
            for n in nodes:
                text = (await n.text_content() or "").strip()
                if not text or text.lower() == "no results found":
                    continue
                node_id = await n.get_attribute("id")
                val = (
                    node_id.split("-")[-1]
                    if (node_id and "-" in node_id)
                    else (await n.get_attribute("data-id")) or text
                )
                out.append((val, text))

            logger.info(f"Found {len(out)} industries")
            return out

        finally:
            await page.close()
            await context.close()
            await browser.close()
            await p.stop()

    async def _apply_industry_filter(self, page, industry_name: str):
        """Mở Select2, chọn đúng ngành theo *tên*, rồi bấm nút btn-company để apply filter."""
        # mở lại dropdown (phòng trường hợp đã đóng)
        sel = page.locator(self.config.get_xpath("industry_select"))
        if await sel.count() == 0:
            sel = page.locator(self.config.get_xpath("industry_select_fallback"))
        await sel.first.dispatch_event("mousedown")

        panel = page.locator(self.config.get_xpath("industry_results"))
        await panel.wait_for(state="visible", timeout=30000)

        # gõ để tìm đúng option (nếu có ô search)
        search_input = page.locator(self.config.get_xpath("industry_search"))
        if await search_input.count():
            await search_input.first.fill(industry_name)
            await asyncio.sleep(0.3)

        # chọn option theo tên (exact match theo normalize-space)
        option = page.locator(
            f"//li[contains(@class,'select2-results__option') and normalize-space()=\"{industry_name}\"]"
        ).first
        if await option.count() == 0:
            # fallback: contains
            option = page.locator(
                f"//li[contains(@class,'select2-results__option')][contains(., \"{industry_name}\")]"
            ).first

        # click option
        try:
            await option.click()
        except Exception:
            # ép click bằng JS nếu bị overlay
            el = await option.element_handle()
            if el:
                await page.evaluate("(el)=>el.click()", el)

        # bấm nút apply filter
        btn = page.locator(self.config.get_xpath("filter_button")).first
        el = await btn.element_handle()
        if el:
            await page.evaluate("(el)=>el.click()", el)
        else:
            # fallback: thử click "force" nếu thấy được
            try:
                await btn.click(force=True)
            except Exception:
                pass

        # chờ load danh sách sau filter
        await page.wait_for_load_state("networkidle", timeout=self.config.processing_config["network_timeout"])
        # đợi anchor công ty xuất hiện (nếu có)
        try:
            await page.wait_for_selector(self.config.get_xpath("company_links"), timeout=5000)
        except Exception:
            # không có cũng không sao; có thể trang trống theo ngành
            pass

    def _build_page_url(self, base_filtered_url: str, page_num: int) -> str:
        """Thêm/ghi đè ?page=N vào URL đã apply filter."""
        if page_num <= 1:
            # giữ nguyên (nếu đã có page param thì đưa về 1)
            if "page=" in base_filtered_url:
                return re.sub(r"([?&])page=\d+", r"\1page=1", base_filtered_url)
            return base_filtered_url
        if "page=" in base_filtered_url:
            return re.sub(r"([?&])page=\d+", rf"\1page={page_num}", base_filtered_url)
        sep = "&" if "?" in base_filtered_url else "?"
        return f"{base_filtered_url}{sep}page={page_num}"

    async def get_company_links_for_page(self, page_url: str):
        # giữ nguyên: mở URL và lôi link "tổng quan"
        for i in range(self.max_retries):
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        headless=True,
                        args=[
                            "--no-sandbox",
                            "--disable-setuid-sandbox",
                            "--disable-dev-shm-usage",
                        ],
                    )
                    context = await browser.new_context()
                    page = await context.new_page()
                    try:
                        await page.goto(
                            page_url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded"
                        )
                        await page.wait_for_load_state("networkidle", timeout=self.config.processing_config["network_timeout"])
                        locs = await page.locator(self.config.get_xpath("company_links")).all()
                        return [
                            await a.get_attribute("href")
                            for a in locs
                            if await a.get_attribute("href")
                        ]
                    finally:
                        await page.close()
                        await context.close()
                        await browser.close()
            except Exception:
                if i == self.max_retries - 1:
                    return []
                await asyncio.sleep(random.uniform(1, 2))

    async def get_company_links_for_industry(
        self, base_url: str, industry_value: str, industry_name: str
    ):
        """
        KHÁC TRƯỚC: không dựa vào ?career_category_id=... nữa.
        Thay vào đó: mở trang -> chọn ngành theo *tên* -> bấm nút 'btn-company' để apply filter,
        rồi dùng URL sau filter để phân trang và gom link.
        """
        p, browser, context, page = await self._open_context()
        try:
            # Mở trang gốc
            await page.goto(base_url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=self.config.processing_config["network_timeout"])

            # Apply filter bằng UI
            await self._apply_industry_filter(page, industry_name)

            # URL sau filter (dùng làm base để thêm page=2,3,…)
            filtered_url = page.url

            # Lấy tổng số trang ngay trên trang hiện tại
            total = await self._get_total_pages_current(page)
        finally:
            await page.close()
            await context.close()
            await browser.close()
            await p.stop()

        # Tạo danh sách URL các trang đã lọc
        page_urls = [self._build_page_url(filtered_url, i) for i in range(1, total + 1)]

        # Gom link "tổng quan" song song theo từng trang
        res = await asyncio.gather(
            *[self.get_company_links_for_page(u) for u in page_urls],
            return_exceptions=True,
        )
        seen, uniq = set(), []
        for r in res:
            if isinstance(r, list):
                for l in r:
                    if l and l not in seen:
                        seen.add(l)
                        uniq.append(l)
        return uniq
