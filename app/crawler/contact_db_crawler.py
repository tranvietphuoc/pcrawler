import asyncio
import random
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

class ContactDBCrawler:
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.db_manager = DatabaseManager()
        self.crawler = None
        
    # Removed _get_crawler() - now using Async Context Manager directly
    
    async def crawl_contact_page(self, url: str, company_name: str, url_type: str) -> bool:
        """Crawl contact page và lưu HTML vào database"""
        if not url or url in ("N/A", ""):
            return False
            
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        # Use Async Context Manager for Crawl4AI crawler
        user_agent = await self._get_random_user_agent()
        async with self.context_manager.get_crawl4ai_crawler(self.crawler_id, user_agent) as crawler:
            try:
                # Thêm init_script cho Facebook
                init_script = None
                if url_type == "facebook":
                    init_script = """
                        // Auto-close Facebook login popup
                        const closePopup = () => {
                            const closeBtn = document.querySelector('[aria-label="Close"], .x1n2onr6.x1ja2u2z, [data-testid="close-button"]');
                            if (closeBtn) {
                                closeBtn.click();
                                console.log('Closed Facebook login popup');
                            }
                            
                            const popup = document.querySelector('[role="dialog"], .x1n2onr6.x1ja2u2z');
                            if (popup) {
                                popup.remove();
                                console.log('Removed Facebook popup');
                            }
                        };
                        
                        closePopup();
                        setTimeout(closePopup, 2000);
                        
                        const observer = new MutationObserver(() => {
                            closePopup();
                        });
                        observer.observe(document.body, { childList: true, subtree: true });
                    """
                
                # Crawl HTML content
                if init_script:
                    result = await crawler.arun(url=url, init_script=init_script)
                else:
                    result = await crawler.arun(url=url)
                    
                html_content = getattr(result, "html", None) or getattr(result, "content", None) or str(result)
                
                if html_content and len(html_content) > 100:  # Valid HTML content
                    # Store to database
                    record_id = self.db_manager.store_contact_html(company_name, url, url_type, html_content)
                    logger.info(f"Stored contact HTML for {company_name}: {url} ({url_type}) (ID: {record_id})")
                    return True
                else:
                    logger.warning(f"Invalid HTML content for {company_name}: {url}")
                    return False
                    
            except Exception as e:
                logger.error(f"Failed to crawl contact page for {company_name}: {url} - {e}")
                return False
    
    async def crawl_batch(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Crawl contact pages cho một batch companies"""
        results = {
            'total': len(companies),
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        for company in companies:
            company_name = company.get('name', '')
            website = company.get('website', '')
            facebook = company.get('facebook', '')
            
            # Crawl website
            if website:
                success = await self.crawl_contact_page(website, company_name, 'website')
                if success:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                results['details'].append({
                    'company': company_name,
                    'url': website,
                    'type': 'website',
                    'success': success
                })
            
            # Crawl Facebook
            if facebook:
                success = await self.crawl_contact_page(facebook, company_name, 'facebook')
                if success:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                results['details'].append({
                    'company': company_name,
                    'url': facebook,
                    'type': 'facebook',
                    'success': success
                })
            
            # Delay between requests
            await asyncio.sleep(random.uniform(1, 3))
        
        return results
    