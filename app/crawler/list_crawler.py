import re, asyncio, random, logging
from typing import List, Tuple
from playwright.async_api import async_playwright
from config import CrawlerConfig
from .base_crawler import BaseCrawler

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ListCrawler(BaseCrawler):
    def __init__(self, config: CrawlerConfig = None, max_retries: int = None, delay_range=None):
        super().__init__(config)
        self.max_retries = max_retries or self.config.processing_config["max_retries"]
        self.delay_range = delay_range or self.config.processing_config["delay_range"]
        self.max_requests_per_browser = 50  # Override for ListCrawler - restart more frequently

    # Removed _open_context() - now using Async Context Manager directly

    async def get_total_pages(self, page, url: str) -> int:
        for i in range(self.max_retries):
            try:
                await page.goto(url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded")
                await self._wait_for_network(page)
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

    async def _wait_for_network(self, page, timeout: int = None):
        """Safely wait for networkidle with fallback to domcontentloaded"""
        if timeout is None:
            timeout = self.config.processing_config["network_timeout"]
        try:
            await page.wait_for_load_state("networkidle", timeout=timeout)
        except Exception as network_error:
            error_str = str(network_error)
            if "Target page, context or browser has been closed" in error_str or "TargetClosedError" in error_str:
                logger.warning(f"Networkidle failed due to browser closure: {network_error}")
                raise
            else:
                logger.warning(f"Networkidle timeout, falling back to domcontentloaded: {network_error}")
                await page.wait_for_load_state("domcontentloaded", timeout=10000)

    async def _wait_for_select2_ready(self, page, max_wait: int = None) -> bool:
        """Đợi Select2 dropdown sẵn sàng với timeout adaptive"""
        if max_wait is None:
            max_wait = self.config.processing_config.get("industry_load_timeout", 45000) // 1000
        
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < max_wait:
            try:
                # Kiểm tra xem select2 có visible không
                select2_selection = page.locator(self.config.get_xpath("industry_select"))
                if await select2_selection.count() == 0:
                    select2_selection = page.locator(self.config.get_xpath("industry_select_fallback"))
                
                if await select2_selection.count() > 0:
                    # Thử click để mở dropdown
                    await select2_selection.first.dispatch_event("mousedown")
                    await asyncio.sleep(1)
                    
                    # Kiểm tra xem dropdown có mở không
                    results_panel = page.locator(self.config.get_xpath("industry_results"))
                    if await results_panel.count() > 0:
                        return True
                
                await asyncio.sleep(2)
            except Exception:
                await asyncio.sleep(2)
                continue
        
        return False

    async def _scroll_and_load_industries(self, page, max_scroll_attempts: int = None) -> int:
        """Scroll và load industries - đơn giản, đảm bảo load đủ 88"""
        # Giảm số lần scroll để tăng tốc độ
        max_scroll_attempts = 80  # Giảm từ 100 xuống 80
        
        prev_count = 0
        stable_count = 0
        max_stable = 2  # Giảm từ 3 xuống 2 để tăng tốc độ
        
        for attempt in range(max_scroll_attempts):
            try:
                # Đếm số industries hiện tại
                current_count = await page.locator(self.config.get_xpath("industry_options")).count()
                
                if current_count == prev_count:
                    stable_count += 1
                    if stable_count >= max_stable:
                        logger.info(f"Industries loaded successfully after {attempt + 1} scroll attempts. Total: {current_count}")
                        return current_count
                else:
                    stable_count = 0
                
                prev_count = current_count
                
                # Scroll xuống cuối
                await page.evaluate(
                    "(sel)=>{const el=document.querySelector(sel); if(el){el.scrollTop=el.scrollHeight;}}",
                    "ul.select2-results__options",
                )
                
                # Giảm delay để tăng tốc độ
                await asyncio.sleep(0.5)  # Giảm từ 1.0s xuống 0.5s
                
                # Force garbage collection mỗi 20 attempts để giảm memory
                if attempt % 20 == 0:
                    import gc
                    gc.collect()
                
            except Exception as e:
                logger.warning(f"Scroll attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(0.5)  # Giảm sleep time
                continue
        
        # Fallback: trả về số lượng hiện tại
        final_count = await page.locator(self.config.get_xpath("industry_options")).count()
        logger.warning(f"Scroll timeout reached. Final count: {final_count}")
        return final_count

    async def get_industries(self, base_url: str) -> List[Tuple[str, str]]:
        """Lấy danh sách industries với retry mechanism và timeout ổn định"""
        max_retries = self.config.processing_config.get("max_retries", 3)
        retry_delay = self.config.processing_config.get("industry_retry_delay", 5)
        
        logger.info(f"Starting industry extraction from: {base_url}")
        logger.info(f"Configuration: max_retries={max_retries}, retry_delay={retry_delay}s")
        
        for retry in range(max_retries):
            try:
                logger.info(f"Attempting to get industries (attempt {retry + 1}/{max_retries})")
                
                # Use Async Context Manager for automatic cleanup
                user_agent = await self._get_random_user_agent()
                viewport = await self._get_random_viewport()
                async with self.context_manager.get_playwright_context(self.crawler_id, user_agent, viewport) as (context, page):
                    try:
                        # 1. Load trang với timeout ổn định
                        timeout = self.config.processing_config.get("timeout", 60000)
                        network_timeout = self.config.processing_config.get("network_timeout", 45000)
                        
                        logger.info(f"Loading page with timeout={timeout}ms, network_timeout={network_timeout}ms")
                        
                        await page.goto(base_url, timeout=timeout, wait_until="domcontentloaded")
                        
                        logger.info("Page loaded, waiting for network idle...")
                        await self._wait_for_network(page, network_timeout)
                        logger.info("Network idle achieved")
                        
                        # 2. Đợi Select2 sẵn sàng
                        logger.info("Waiting for Select2 to be ready...")
                        if not await self._wait_for_select2_ready(page):
                            raise Exception("Select2 dropdown not ready after timeout")
                        logger.info("Select2 dropdown is ready")
                        
                        # 3. Scroll và load industries
                        logger.info("Scrolling to load all industries...")
                        total_industries = await self._scroll_and_load_industries(page)
                        
                        if total_industries == 0:
                            raise Exception("No industries found after scrolling")
                        
                        logger.info(f"Scrolling completed. Found {total_industries} industry options")
                        
                        # 4. Thu thập industries
                        logger.info("Extracting industry data...")
                        nodes = await page.locator(self.config.get_xpath("industry_options")).all()
                        logger.info(f"Processing {len(nodes)} industry nodes")
                        
                        out: List[Tuple[str, str]] = []
                        skipped_count = 0
                        
                        for i, n in enumerate(nodes):
                            try:
                                text = (await n.text_content() or "").strip()
                                if not text or text.lower() in ["no results found", "loading...", ""]:
                                    skipped_count += 1
                                    continue
                                
                                node_id = await n.get_attribute("id")
                                val = (
                                    node_id.split("-")[-1]
                                    if (node_id and "-" in node_id)
                                    else (await n.get_attribute("data-id")) or text
                                )
                                out.append((val, text))
                                
                                if (i + 1) % 10 == 0:  # Log progress mỗi 10 items
                                    logger.info(f"Processed {i + 1}/{len(nodes)} industries...")
                                    
                            except Exception as e:
                                logger.warning(f"Failed to process industry node {i}: {e}")
                                skipped_count += 1
                                continue
                        
                        if len(out) == 0:
                            raise Exception("No valid industries extracted")
                        
                        logger.info(f"Industry extraction completed: {len(out)} valid, {skipped_count} skipped")
                        logger.info(f"Successfully found {len(out)} industries")
                        return out
                        
                    except Exception as e:
                        logger.error(f"Error in get_industries: {e}")
                        raise
                    # Context and page are automatically closed here
                    
            except Exception as e:
                logger.error(f"Attempt {retry + 1} failed: {e}")
                
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * retry_delay  # Tăng thời gian chờ mỗi lần retry
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("All attempts failed. Returning empty list.")
                    return []

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
        await self._wait_for_network(page)
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

    async def get_company_links_for_page(self, page_url: str, page=None):
        # Sử dụng lại page đã có hoặc tạo mới nếu cần
        if page is None:
            # Fallback: tạo browser mới nếu không có page
            for i in range(self.max_retries):
                try:
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(
                            headless=True,
                            args=[
                                "--no-sandbox",
                                "--disable-setuid-sandbox",
                                "--disable-dev-shm-usage",
                                "--memory-pressure-off",  # Tắt memory pressure
                                "--max_old_space_size=4096",  # Tăng heap size
                            ],
                        )
                        context = await browser.new_context()
                        page = await context.new_page()
                        # Giảm timeout cho từng trang để tránh bị stuck
                        page_timeout = min(self.config.processing_config["timeout"], 30000)  # Max 30s per page
                        await page.goto(
                            page_url, timeout=page_timeout, wait_until="domcontentloaded"
                        )
                        await self._wait_for_network(page)
                        locs = await page.locator(self.config.get_xpath("company_links")).all()
                        return [
                            await a.get_attribute("href")
                            for a in locs
                            if await a.get_attribute("href")
                        ]
                        # Note: page, context, browser are closed automatically by Async Context Manager
                except Exception:
                    if i == self.max_retries - 1:
                        return []
                    await asyncio.sleep(random.uniform(1, 2))
        else:
            # Sử dụng page đã có
            try:
                await page.goto(
                    page_url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded"
                )
                await self._wait_for_network(page)
                locs = await page.locator(self.config.get_xpath("company_links")).all()
                return [
                    await a.get_attribute("href")
                    for a in locs
                    if await a.get_attribute("href")
                ]
            except Exception:
                return []

    async def get_company_links_for_industry(
        self, base_url: str, industry_value: str, industry_name: str
    ):
        """
        mở trang -> chọn ngành theo *tên* -> bấm nút 'btn-company' để apply filter,
        rồi dùng URL sau filter để phân trang và gom link.
        """
        # Use Async Context Manager for automatic cleanup
        user_agent = await self._get_random_user_agent()
        viewport = await self._get_random_viewport()
        async with self.context_manager.get_playwright_context(self.crawler_id, user_agent, viewport) as (context, page):
            try:
                # Mở trang gốc
                await page.goto(base_url, timeout=self.config.processing_config["timeout"], wait_until="domcontentloaded")
                
                # Wait for network idle with safe fallback
                await self._wait_for_network(page)

                # Apply filter bằng UI
                await self._apply_industry_filter(page, industry_name)

                # URL sau filter (dùng làm base để thêm page=2,3,…)
                filtered_url = page.url

                # Lấy tổng số trang ngay trên trang hiện tại
                total = await self._get_total_pages_current(page)

                # Tạo danh sách URL các trang đã lọc (ước lượng ban đầu)
                page_urls = [self._build_page_url(filtered_url, i) for i in range(1, max(total, 1) + 1)]

            except Exception as e:
                logger.error(f"Error in get_company_links_for_industry: {e}")
                raise
            # Context and page are automatically closed here

        # Gom link song song: 1 browser/worker, tạo nhiều context/page song song qua context_manager
        seen, uniq = set(), []

        concurrency = 6  # 32GB: 6 context song song
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_context(url: str) -> List[str]:
            max_attempts = 3
            for attempt in range(max_attempts):
                user_agent = await self._get_random_user_agent()
                viewport = await self._get_random_viewport()
                try:
                    async with self.context_manager.get_playwright_context(self.crawler_id, user_agent, viewport) as (context, page):
                        page.set_default_timeout(25000)
                        try:
                            await page.goto(url, timeout=20000, wait_until="domcontentloaded")
                        except Exception as goto_error:
                            if "TargetClosedError" in str(goto_error) or "has been closed" in str(goto_error):
                                raise
                        try:
                            await page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            try:
                                await page.wait_for_load_state("domcontentloaded", timeout=5000)
                            except Exception:
                                pass
                        locs = await page.locator(self.config.get_xpath("company_links")).all()
                        links = []
                        for a in locs:
                            try:
                                href = await a.get_attribute("href")
                                if href:
                                    links.append(href)
                            except Exception:
                                continue
                        return links
                except Exception as e:
                    msg = str(e)
                    if attempt < max_attempts - 1 and ("TargetClosedError" in msg or "has been closed" in msg or "Timeout" in msg):
                        await asyncio.sleep(1.0 * (attempt + 1))
                        continue
                    return []

        async def worker(url: str):
            async with semaphore:
                links = await fetch_with_context(url)
                if links:
                    for link in links:
                        if link and link not in seen:
                            seen.add(link)
                            uniq.append(link)

        # Suspend auto restarts trong batch để không đóng browser khi đang chạy
        await self.context_manager.suspend_restarts()
        try:
            # 1) Chạy batch đầu theo tổng trang ước lượng
            tasks = [asyncio.create_task(worker(u)) for u in page_urls]
            completed = 0
            for coro in asyncio.as_completed(tasks):
                await coro
                completed += 1
                if completed % 10 == 0 or completed == len(tasks):
                    logger.info(f"Collected pages (est): {completed}/{len(tasks)} | unique: {len(uniq)} | industry: {industry_name}")

            # 2) Mở rộng phân trang cho đến khi thật sự hết
            # Quy tắc dừng: gặp 2 trang liên tiếp không thêm link mới
            consecutive_empty = 0
            current_page = len(page_urls) + 1
            while consecutive_empty < 2:
                url = self._build_page_url(filtered_url, current_page)
                before_count = len(uniq)
                await worker(url)
                added = len(uniq) - before_count
                if added == 0:
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0
                if current_page % 10 == 0:
                    logger.info(f"Extended scan p{current_page}, unique: {len(uniq)} | empty_streak: {consecutive_empty}")
                current_page += 1
        finally:
            await self.context_manager.resume_restarts()

        import gc
        gc.collect()

        return uniq
    
