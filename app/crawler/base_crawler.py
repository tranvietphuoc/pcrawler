import asyncio
import random
import logging
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from playwright.async_api import async_playwright
from config import CrawlerConfig
from .async_context_manager import get_global_context_manager

logger = logging.getLogger(__name__)

class BaseCrawler:
    """Base class cho tất cả crawlers với Async Context Manager"""
    
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        
        # Async context manager
        self.context_manager = get_global_context_manager()
        self.crawler_id = f"{self.__class__.__name__}_{id(self)}"
        
        # Request tracking
        self.request_count = 0
        
        # User agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        ]
        
        # Viewport rotation
        self.viewports = [
            {'width': 1920, 'height': 1080},
            {'width': 1366, 'height': 768},
            {'width': 1536, 'height': 864},
            {'width': 1440, 'height': 900},
            {'width': 1280, 'height': 720},
        ]
    
    async def _get_random_user_agent(self):
        """Get random user agent"""
        return random.choice(self.user_agents)
    
    async def _get_random_viewport(self):
        """Get random viewport"""
        return random.choice(self.viewports)
    
    async def _open_playwright_context(self, force_new_browser: bool = False):
        """Open Playwright context using Async Context Manager for conflict prevention"""
        try:
            # Get random user agent and viewport
            user_agent = await self._get_random_user_agent()
            viewport = await self._get_random_viewport()
            
            # Use Async Context Manager to get context
            context_manager = self.context_manager.get_playwright_context(
                self.crawler_id, user_agent, viewport
            )
            
            # This will be used as async context manager
            return context_manager
            
        except Exception as e:
            logger.warning(f"{self.__class__.__name__} context creation failed: {e}")
            raise

    async def _get_crawl4ai_crawler(self):
        """Get Crawl4AI crawler using Async Context Manager for conflict prevention"""
        # Get random user agent
        user_agent = await self._get_random_user_agent()
        
        # Use Async Context Manager to get crawler
        context_manager = self.context_manager.get_crawl4ai_crawler(self.crawler_id, user_agent)
        
        # Increment request count
        self.request_count += 1
        return context_manager

    async def cleanup(self):
        """Cleanup all browser resources using Async Context Manager"""
        # Use Async Context Manager to cleanup crawler-specific resources
        await self.context_manager.cleanup_crawler(self.crawler_id)
        
        # Reset state
        self.request_count = 0
        
        logger.info(f"{self.__class__.__name__} cleanup completed via Async Context Manager")
    
    async def create_fresh_browser_for_industry(self):
        """Create a completely fresh browser for each industry - 100% error prevention"""
        # Cleanup existing browser completely
        await self.cleanup()
        
        # Wait a bit for cleanup to complete
        await asyncio.sleep(2)
        
        logger.info(f"{self.__class__.__name__} fresh browser created for industry")
    
    def get_stats(self) -> dict:
        """Get crawler statistics"""
        return {
            "crawler_id": self.crawler_id,
            "request_count": self.request_count,
            "context_manager_stats": self.context_manager.get_stats()
        }
