import asyncio
import random
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

class DetailDBCrawler:
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.db_manager = DatabaseManager()
        self.crawler = None
        
    async def _get_crawler(self):
        """Lazy initialization của crawler"""
        if not self.crawler:
            self.crawler = AsyncWebCrawler()
        return self.crawler
    
    async def crawl_detail_page(self, company_url: str, company_name: str) -> bool:
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
                record_id = self.db_manager.store_detail_html(company_name, company_url, html_content)
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
            company_name = company.get('name', '')
            company_url = company.get('url', '')
            
            if company_url:
                success = await self.crawl_detail_page(company_url, company_name)
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
    
    def cleanup(self):
        """Cleanup crawler resources"""
        if self.crawler:
            try:
                # Note: AsyncWebCrawler.close() is async, but this method is sync
                # In real implementation, you'd need to handle this properly
                pass
            except Exception as e:
                logger.error(f"Error cleaning up detail crawler: {e}")
            finally:
                self.crawler = None
