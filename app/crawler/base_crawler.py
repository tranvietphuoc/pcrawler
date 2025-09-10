import asyncio
import random
import logging
from typing import List, Dict, Any
from playwright.async_api import async_playwright
from config import CrawlerConfig
from .async_context_manager import get_context_manager

logger = logging.getLogger(__name__)

class BaseCrawler:
    """Base class cho tất cả crawlers với Async Context Manager"""
    
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        
        # Async context manager - NEW INSTANCE for isolation
        self.context_manager = get_context_manager()
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
    
    # Removed _open_playwright_context() - now using context_manager.get_playwright_context() directly

    # Removed _get_crawl4ai_crawler() - now using context_manager.get_crawl4ai_crawler() directly

    async def cleanup(self):
        """Cleanup all browser resources using Async Context Manager"""
        # Use Async Context Manager to cleanup all resources
        await self.context_manager.cleanup()
        
        # Reset state
        self.request_count = 0
        
        logger.info(f"{self.__class__.__name__} cleanup completed via Async Context Manager")
    
    
    def get_stats(self) -> dict:
        """Get crawler statistics"""
        return {
            "crawler_id": self.crawler_id,
            "request_count": self.request_count,
            "context_manager_stats": self.context_manager.get_stats()
        }
