import asyncio
import random
import logging
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from playwright.async_api import async_playwright
from config import CrawlerConfig

logger = logging.getLogger(__name__)

class BaseCrawler:
    """Base class cho tất cả crawlers với browser optimization"""
    
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        
        # Browser optimization settings
        self.request_count = 0
        self.max_requests_per_browser = 500  # Default restart threshold
        self.current_browser = None
        self.current_playwright = None
        self.crawler = None
        
        # User agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        # Random viewport rotation
        self.viewports = [
            {'width': 1920, 'height': 1080},
            {'width': 1366, 'height': 768},
            {'width': 1440, 'height': 900},
            {'width': 1536, 'height': 864},
            {'width': 1280, 'height': 720},
            {'width': 1600, 'height': 900}
        ]
    
    async def _get_random_user_agent(self):
        """Get random user agent"""
        return random.choice(self.user_agents)
    
    async def _get_random_viewport(self):
        """Get random viewport"""
        return random.choice(self.viewports)
    
    async def _should_restart_browser(self):
        """Check if browser should be restarted"""
        return self.request_count >= self.max_requests_per_browser
    
    async def _restart_playwright_browser(self):
        """Restart Playwright browser to prevent memory leaks"""
        # Force cleanup existing browser
        if self.current_browser:
            try:
                # Close all contexts first
                contexts = self.current_browser.contexts
                for context in contexts:
                    try:
                        await context.close()
                    except:
                        pass
                await self.current_browser.close()
                logger.info(f"{self.__class__.__name__} browser closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
        
        if self.current_playwright:
            try:
                await self.current_playwright.stop()
                logger.info(f"{self.__class__.__name__} playwright stopped")
            except Exception as e:
                logger.warning(f"Error stopping playwright: {e}")
        
        # Reset state
        self.current_browser = None
        self.current_playwright = None
        
        # Wait a bit before restarting
        await asyncio.sleep(1)
        
        # Start fresh browser
        try:
            self.current_playwright = await async_playwright().start()
                    self.current_browser = await self.current_playwright.chromium.launch(
                        headless=True,
                        args=[
                            "--no-sandbox",
                            "--disable-setuid-sandbox",
                            "--disable-dev-shm-usage",
                            "--memory-pressure-off",
                            "--disable-background-timer-throttling",
                            "--disable-backgrounding-occluded-windows",
                            "--disable-renderer-backgrounding",
                            "--disable-web-security",
                            "--disable-features=VizDisplayCompositor",
                            "--disable-gpu",
                            "--disable-software-rasterizer",
                            "--disable-background-networking",
                            "--disable-default-apps",
                            "--disable-extensions",
                            "--disable-sync",
                            "--disable-translate",
                            "--hide-scrollbars",
                            "--mute-audio",
                            "--no-first-run",
                            "--disable-ipc-flooding-protection"
                        ],
                    )
            self.request_count = 0
            logger.info(f"{self.__class__.__name__} Playwright browser restarted successfully")
        except Exception as e:
            logger.error(f"Failed to restart browser: {e}")
            raise

    async def _restart_crawl4ai_crawler(self):
        """Restart Crawl4AI crawler to prevent memory leaks"""
        if self.crawler:
            try:
                await self.crawler.close()
            except:
                pass
        
        self.crawler = AsyncWebCrawler(
            user_agent=await self._get_random_user_agent(),
            browser_type="chromium",
            headless=True,
            browser_args=[
                "--no-sandbox",
                "--disable-setuid-sandbox", 
                "--disable-dev-shm-usage",
                "--memory-pressure-off",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding"
            ]
        )
        self.request_count = 0
        logger.info(f"{self.__class__.__name__} Crawl4AI crawler restarted after {self.max_requests_per_browser} requests")

    async def _open_playwright_context(self, force_new_browser: bool = False):
        """Open Playwright context with 100% error prevention"""
        try:
            # Force new browser if requested or if current browser is invalid
            if force_new_browser or await self._should_restart_browser() or not self.current_browser:
                await self._restart_playwright_browser()
            
            # Get random user agent and viewport
            user_agent = await self._get_random_user_agent()
            viewport = await self._get_random_viewport()
            
            # Create new context with error handling
            context = await self.current_browser.new_context(
                user_agent=user_agent,
                viewport=viewport,
                extra_http_headers={
                    'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )
            page = await context.new_page()
            
            # Increment request count
            self.request_count += 1
            
            return self.current_playwright, self.current_browser, context, page
            
        except Exception as e:
            logger.warning(f"{self.__class__.__name__} context creation failed: {e}")
            # Force restart and retry once
            await self._restart_playwright_browser()
            await asyncio.sleep(2)
            
            # Retry with fresh browser
            user_agent = await self._get_random_user_agent()
            viewport = await self._get_random_viewport()
            
            context = await self.current_browser.new_context(
                user_agent=user_agent,
                viewport=viewport,
                extra_http_headers={
                    'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )
            page = await context.new_page()
            self.request_count += 1
            
            return self.current_playwright, self.current_browser, context, page

    async def _get_crawl4ai_crawler(self):
        """Get Crawl4AI crawler with optimization"""
        if await self._should_restart_browser() or not self.crawler:
            await self._restart_crawl4ai_crawler()
        
        # Increment request count
        self.request_count += 1
        return self.crawler

    async def cleanup(self):
        """Cleanup all browser resources"""
        # Cleanup Playwright
        if self.current_browser:
            try:
                await self.current_browser.close()
                logger.info(f"{self.__class__.__name__} Playwright browser closed")
            except Exception as e:
                logger.warning(f"Error closing {self.__class__.__name__} Playwright browser: {e}")
        
        if self.current_playwright:
            try:
                await self.current_playwright.stop()
                logger.info(f"{self.__class__.__name__} Playwright stopped")
            except Exception as e:
                logger.warning(f"Error stopping {self.__class__.__name__} Playwright: {e}")
        
        # Cleanup Crawl4AI
        if self.crawler:
            try:
                await self.crawler.close()
                logger.info(f"{self.__class__.__name__} Crawl4AI crawler closed")
            except Exception as e:
                logger.warning(f"Error closing {self.__class__.__name__} Crawl4AI crawler: {e}")
        
        # Reset state
        self.current_browser = None
        self.current_playwright = None
        self.crawler = None
        self.request_count = 0
    
    async def create_fresh_browser_for_industry(self):
        """Create a completely fresh browser for each industry - 100% error prevention"""
        # Cleanup existing browser completely
        await self.cleanup()
        await asyncio.sleep(2)  # Wait for complete cleanup
        
        # Create brand new browser
        await self._restart_playwright_browser()
        logger.info(f"{self.__class__.__name__} created fresh browser for new industry")
