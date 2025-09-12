import asyncio
import random
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
from .base_crawler import BaseCrawler
import logging

logger = logging.getLogger(__name__)

class DetailCrawler(BaseCrawler):
    def __init__(self, config: CrawlerConfig = None):
        super().__init__(config)
        self.db_manager = DatabaseManager()
        self.max_requests_per_browser = 100  # Override for DetailCrawler - balance memory vs stability
        
    # Removed _get_crawler() - now using context_manager.get_crawl4ai_crawler() directly
    
    async def crawl_detail_page(self, company_url: str, company_name: str, industry: str = None) -> bool:
        """Crawl detail page và lưu HTML vào database"""
        if not company_url or company_url in ("N/A", ""):
            return False
            
        if not company_url.startswith(("http://", "https://")):
            company_url = "https://" + company_url
        
        # Random delay between requests
        await asyncio.sleep(random.uniform(1, 3))
        
        # Use Async Context Manager for Crawl4AI crawler
        user_agent = await self._get_random_user_agent()
        viewport = await self._get_random_viewport()
        async with self.context_manager.get_crawl4ai_crawler(self.crawler_id, user_agent, viewport) as crawler:
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
        """Crawl batch of companies with deduplication - skip already crawled URLs"""
        if not companies:
            return {'total': 0, 'successful': 0, 'failed': 0, 'skipped': 0}
        
        # Extract URLs for batch checking
        urls = []
        for company in companies:
            url = company.get('url', '')
            if url and url not in ("N/A", ""):
                if not url.startswith(("http://", "https://")):
                    url = "https://" + url
                urls.append(url)
        
        # Batch check which URLs already exist
        existing_urls = set()
        if urls:
            url_exists_map = self.db_manager.check_urls_exist_batch(urls)
            existing_urls = {url for url, exists in url_exists_map.items() if exists}
        
        logger.info(f"Batch deduplication: {len(existing_urls)}/{len(urls)} URLs already exist, will skip")
        
        # Filter out existing URLs
        new_companies = []
        skipped_count = 0
        for company in companies:
            url = company.get('url', '')
            if url and url not in ("N/A", ""):
                if not url.startswith(("http://", "https://")):
                    url = "https://" + url
                if url in existing_urls:
                    skipped_count += 1
                    logger.debug(f"Skipping already crawled URL: {url}")
                    continue
            new_companies.append(company)
        
        logger.info(f"After deduplication: {len(new_companies)} new companies to crawl, {skipped_count} skipped")
        
        if not new_companies:
            return {'total': len(companies), 'successful': 0, 'failed': 0, 'skipped': skipped_count}
        
        # Crawl only new companies (after deduplication)
        results = {
            'total': len(companies),
            'successful': 0,
            'failed': 0,
            'skipped': skipped_count,
            'details': []
        }
        
        for company in new_companies:
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
    
