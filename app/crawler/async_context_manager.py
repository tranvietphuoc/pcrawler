import asyncio
import logging
import time
from typing import Dict, List, Optional, AsyncContextManager
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from crawl4ai import AsyncWebCrawler

logger = logging.getLogger(__name__)

class AsyncBrowserContextManager:
    """Async context manager for browser resources with proper lifecycle management"""
    
    def __init__(self, max_browsers: int = 3, max_contexts_per_browser: int = 2):
        self._browsers: Dict[str, Browser] = {}
        self._contexts: Dict[str, List[BrowserContext]] = {}
        self._crawl4ai_crawlers: Dict[str, AsyncWebCrawler] = {}
        self._active_contexts: Dict[str, int] = {}  # Track active contexts per browser
        self._max_browsers = max_browsers
        self._max_contexts_per_browser = max_contexts_per_browser
        self._lock = asyncio.Lock()
        self._context_lifetime = 30  # seconds
    
    @asynccontextmanager
    async def get_playwright_context(self, crawler_id: str, user_agent: str, viewport: dict):
        """Async context manager for Playwright context with automatic cleanup"""
        context = None
        page = None
        
        try:
            async with self._lock:
                # Get or create browser
                browser = await self._get_or_create_browser(crawler_id)
                
                # Create new context
                context = await browser.new_context(
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
                
                # Track active context
                self._active_contexts[crawler_id] = self._active_contexts.get(crawler_id, 0) + 1
                
                # Create page
                page = await context.new_page()
                
                logger.debug(f"Created context for {crawler_id}, active contexts: {self._active_contexts[crawler_id]}")
                
                yield context, page
                
        except Exception as e:
            logger.error(f"Error in playwright context for {crawler_id}: {e}")
            raise
        finally:
            # Cleanup context
            if context:
                try:
                    await context.close()
                    async with self._lock:
                        self._active_contexts[crawler_id] = max(0, self._active_contexts.get(crawler_id, 0) - 1)
                    logger.debug(f"Closed context for {crawler_id}, active contexts: {self._active_contexts.get(crawler_id, 0)}")
                except Exception as e:
                    logger.warning(f"Error closing context for {crawler_id}: {e}")
    
    @asynccontextmanager
    async def get_crawl4ai_crawler(self, crawler_id: str, user_agent: str):
        """Async context manager for Crawl4AI crawler with automatic cleanup"""
        crawler = None
        
        try:
            async with self._lock:
                # Get or create crawler
                crawler = await self._get_or_create_crawl4ai_crawler(crawler_id, user_agent)
                
                logger.debug(f"Using Crawl4AI crawler for {crawler_id}")
                
                yield crawler
                
        except Exception as e:
            logger.error(f"Error in Crawl4AI crawler for {crawler_id}: {e}")
            raise
        finally:
            # Note: Crawl4AI crawler is reused, not closed here
            pass
    
    async def _get_or_create_browser(self, crawler_id: str) -> Browser:
        """Get or create browser with proper resource management"""
        if crawler_id not in self._browsers:
            # Check if we need to close an old browser
            if len(self._browsers) >= self._max_browsers:
                await self._close_oldest_browser()
            
            # Create new browser
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--memory-pressure-off",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--disable-features=TranslateUI",
                    "--disable-ipc-flooding-protection",
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
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor",
                ]
            )
            self._browsers[crawler_id] = browser
            self._active_contexts[crawler_id] = 0
            logger.info(f"Created new browser for {crawler_id}")
        
        return self._browsers[crawler_id]
    
    async def _get_or_create_crawl4ai_crawler(self, crawler_id: str, user_agent: str) -> AsyncWebCrawler:
        """Get or create Crawl4AI crawler with proper resource management"""
        if crawler_id not in self._crawl4ai_crawlers:
            # Check if we need to close an old crawler
            if len(self._crawl4ai_crawlers) >= self._max_browsers:
                await self._close_oldest_crawl4ai_crawler()
            
            # Create new Crawl4AI crawler
            crawler = AsyncWebCrawler(
                user_agent=user_agent,
                browser_type="chromium",
                headless=True,
                browser_args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--memory-pressure-off",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--disable-features=TranslateUI",
                    "--disable-ipc-flooding-protection",
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
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor",
                ]
            )
            self._crawl4ai_crawlers[crawler_id] = crawler
            logger.info(f"Created new Crawl4AI crawler for {crawler_id}")
        
        return self._crawl4ai_crawlers[crawler_id]
    
    async def _close_oldest_browser(self):
        """Close the oldest browser to make room for new one"""
        if not self._browsers:
            return
        
        # Find browser with least active contexts
        oldest_id = min(self._browsers.keys(), key=lambda k: self._active_contexts.get(k, 0))
        
        try:
            await self._browsers[oldest_id].close()
            del self._browsers[oldest_id]
            del self._active_contexts[oldest_id]
            logger.info(f"Closed oldest browser: {oldest_id}")
        except Exception as e:
            logger.warning(f"Error closing browser {oldest_id}: {e}")
    
    async def _close_oldest_crawl4ai_crawler(self):
        """Close the oldest Crawl4AI crawler to make room for new one"""
        if not self._crawl4ai_crawlers:
            return
        
        # Find oldest crawler
        oldest_id = min(self._crawl4ai_crawlers.keys(), key=lambda k: time.time())
        
        try:
            await self._crawl4ai_crawlers[oldest_id].close()
            del self._crawl4ai_crawlers[oldest_id]
            logger.info(f"Closed oldest Crawl4AI crawler: {oldest_id}")
        except Exception as e:
            logger.warning(f"Error closing Crawl4AI crawler {oldest_id}: {e}")
    
    async def cleanup_crawler(self, crawler_id: str):
        """Cleanup all resources for specific crawler"""
        async with self._lock:
            # Close browser
            if crawler_id in self._browsers:
                try:
                    await self._browsers[crawler_id].close()
                    del self._browsers[crawler_id]
                    del self._active_contexts[crawler_id]
                    logger.info(f"Closed browser for {crawler_id}")
                except Exception as e:
                    logger.warning(f"Error closing browser for {crawler_id}: {e}")
            
            # Close Crawl4AI crawler
            if crawler_id in self._crawl4ai_crawlers:
                try:
                    await self._crawl4ai_crawlers[crawler_id].close()
                    del self._crawl4ai_crawlers[crawler_id]
                    logger.info(f"Closed Crawl4AI crawler for {crawler_id}")
                except Exception as e:
                    logger.warning(f"Error closing Crawl4AI crawler for {crawler_id}: {e}")
    
    async def cleanup_all(self):
        """Cleanup all browser resources"""
        async with self._lock:
            # Close all browsers
            for crawler_id in list(self._browsers.keys()):
                try:
                    await self._browsers[crawler_id].close()
                except:
                    pass
            
            # Close all Crawl4AI crawlers
            for crawler_id in list(self._crawl4ai_crawlers.keys()):
                try:
                    await self._crawl4ai_crawlers[crawler_id].close()
                except:
                    pass
            
            self._browsers.clear()
            self._crawl4ai_crawlers.clear()
            self._active_contexts.clear()
            
            logger.info("All browser resources cleaned up")
    
    def get_stats(self) -> dict:
        """Get resource usage statistics"""
        return {
            "browsers": len(self._browsers),
            "crawl4ai_crawlers": len(self._crawl4ai_crawlers),
            "active_contexts": sum(self._active_contexts.values()),
            "max_browsers": self._max_browsers,
            "max_contexts_per_browser": self._max_contexts_per_browser
        }

# Global instance
_global_context_manager = None

def get_global_context_manager() -> AsyncBrowserContextManager:
    """Get global async context manager instance"""
    global _global_context_manager
    if _global_context_manager is None:
        _global_context_manager = AsyncBrowserContextManager()
    return _global_context_manager
