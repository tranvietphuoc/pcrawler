import asyncio
import random
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
from .base_crawler import BaseCrawler
import logging

logger = logging.getLogger(__name__)

class DetailDBCrawler(BaseCrawler):
    def __init__(self, config: CrawlerConfig = None):
        super().__init__(config)
        self.db_manager = DatabaseManager()
        self.max_requests_per_browser = 200  # Override for DetailDBCrawler
        
    async def _get_crawler(self):
        """Get Crawl4AI crawler using base class method"""
        return await self._get_crawl4ai_crawler()
    
    async def crawl_detail_page(self, company_url: str, company_name: str, industry: str = None) -> bool:
        """Crawl detail page và lưu HTML vào database"""
        if not company_url or company_url in ("N/A", ""):
            return False
            
        if not company_url.startswith(("http://", "https://")):
            company_url = "https://" + company_url
        
        crawler = await self._get_crawler()
        
        try:
            # Crawl detail page HTML content
            result = await crawler.arun(url=company_url)
            html_content = getattr(result, "html", None) or getattr(result, "content", None) or str(result)
            
            if html_content and len(html_content) > 100:  # Valid HTML content
                # Store to database
                record_id = self.db_manager.store_detail_html(company_name, company_url, html_content, industry)
                logger.info(f"Stored detail HTML for {company_name}: {company_url} (ID: {record_id})")
                return True
            else:
                logger.warning(f"Invalid HTML content for {company_name}: {company_url}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to crawl detail page for {company_name}: {company_url} - {e}")
            return False
    
    async def crawl_batch(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Crawl detail pages cho một batch companies"""
        results = {
            'total': len(companies),
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        for company in companies:
            # Support both dict and plain URL string
            if isinstance(company, str):
                company_url = company
                company_name = ''
                industry = None
            elif isinstance(company, dict):
                company_name = company.get('name', '')
                company_url = company.get('url', '')
                industry = company.get('industry')
            else:
                company_name = ''
                company_url = ''
                industry = None
            
            if company_url:
                success = await self.crawl_detail_page(company_url, company_name, industry)
                if success:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                results['details'].append({
                    'company': company_name,
                    'url': company_url,
                    'success': success
                })
            
            # Delay between requests
            await asyncio.sleep(random.uniform(1, 3))
        
        return results
    
