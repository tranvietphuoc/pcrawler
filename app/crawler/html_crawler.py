import asyncio
import random
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

class HTMLCrawler:
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.db_manager = DatabaseManager()
        self.crawler = None
        
    async def _get_crawler(self):
        """Lazy initialization của crawler"""
        if not self.crawler:
            self.crawler = AsyncWebCrawler()
        return self.crawler
    
    async def crawl_html(self, url: str, company_name: str, url_type: str = 'website') -> bool:
        """Crawl HTML content và lưu vào database (contact_html_storage)"""
        if not url or url in ("N/A", ""):
            return False
            
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        crawler = await self._get_crawler()
        
        try:
            # Crawl HTML content
            result = await crawler.arun(url=url)
            html_content = getattr(result, "html", None) or getattr(result, "content", None) or str(result)
            
            if html_content and len(html_content) > 100:  # Valid HTML content
                # Store to database (contact)
                record_id = self.db_manager.store_contact_html(company_name, url, url_type, html_content)
                logger.info(f"Stored {url_type} HTML for {company_name}: {url} (ID: {record_id})")
                return True
            else:
                logger.warning(f"Invalid HTML content for {company_name}: {url}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to crawl HTML for {company_name}: {url} - {e}")
            return False
    
    async def crawl_batch(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Crawl HTML cho một batch companies"""
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
            
            # Crawl website (main URL)
            if website:
                success = await self.crawl_html(website, company_name, 'website')
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
            
            # Crawl Facebook (main URL)
            if facebook:
                success = await self.crawl_html(facebook, company_name, 'facebook')
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
    
    async def crawl_batch_from_details(self, company_details: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Crawl contact pages từ company details đã extract (deep crawl website + facebook)"""
        results = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        processing = self.config.get_processing_config()
        deep_paths = processing.get('website_deep_paths', [])
        fb_variants = processing.get('facebook_about_variants', [])

        for company in company_details:
            company_name = company.get('company_name', '')
            website = company.get('website', '')
            facebook = company.get('facebook', '')
            
            # Crawl website (main URL)
            if website:
                results['total'] += 1
                success = await self.crawl_html(website, company_name, 'website')
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

                # Deep paths for website
                for path in deep_paths:
                    try:
                        candidate = website.rstrip('/') + path
                        results['total'] += 1
                        success = await self.crawl_html(candidate, company_name, 'website')
                        if success:
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                        results['details'].append({
                            'company': company_name,
                            'url': candidate,
                            'type': 'website',
                            'success': success
                        })
                        await asyncio.sleep(random.uniform(1, 2))
                    except Exception as e:
                        logger.debug(f"Deep path crawl error {company_name} {path}: {e}")
            
            # Crawl Facebook (với auto close login window)
            if facebook:
                results['total'] += 1
                success = await self.crawl_facebook_with_close_login(facebook, company_name)
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

                # Crawl Facebook about variants
                for variant in fb_variants:
                    try:
                        candidate = facebook.rstrip('/') + variant
                        results['total'] += 1
                        success = await self.crawl_facebook_with_close_login(candidate, company_name)
                        if success:
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                        results['details'].append({
                            'company': company_name,
                            'url': candidate,
                            'type': 'facebook',
                            'success': success
                        })
                        await asyncio.sleep(random.uniform(1, 2))
                    except Exception as e:
                        logger.debug(f"Facebook variant crawl error {company_name} {variant}: {e}")
            
            # Delay between requests
            await asyncio.sleep(random.uniform(1, 3))
        
        return results
    
    async def crawl_facebook_with_close_login(self, url: str, company_name: str) -> bool:
        """Crawl Facebook page với auto close login window"""
        if not url or url in ("N/A", ""):
            return False
            
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        crawler = await self._get_crawler()
        
        try:
            # Crawl Facebook page với auto close login
            result = await crawler.arun(
                url=url,
                js_code="""
                    // Auto close login popup nếu có
                    setTimeout(() => {
                        // Tìm và click close button
                        const closeButtons = document.querySelectorAll('[aria-label="Close"], [aria-label="Đóng"], .close, .x');
                        for (let btn of closeButtons) {
                            if (btn.offsetParent !== null) { // Visible
                                btn.click();
                                break;
                            }
                        }
                        
                        // Tìm và click "Not Now" button
                        const notNowButtons = document.querySelectorAll('[aria-label="Not Now"], [aria-label="Không phải bây giờ"]');
                        for (let btn of notNowButtons) {
                            if (btn.offsetParent !== null) { // Visible
                                btn.click();
                                break;
                            }
                        }
                    }, 2000);
                """,
                wait_for="networkidle"
            )
            
            html_content = getattr(result, "html", None) or getattr(result, "content", None) or str(result)
            
            if html_content and len(html_content) > 100:  # Valid HTML content
                # Store to database
                record_id = self.db_manager.store_contact_html(company_name, url, 'facebook', html_content)
                logger.info(f"Stored Facebook HTML for {company_name}: {url} (ID: {record_id})")
                return True
            else:
                logger.warning(f"Invalid Facebook HTML content for {company_name}: {url}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to crawl Facebook page for {company_name}: {url} - {e}")
            return False
    
    def cleanup(self):
        """Cleanup crawler resources"""
        if self.crawler:
            try:
                # Note: AsyncWebCrawler.close() is async, but this method is sync
                # In real implementation, you'd need to handle this properly
                pass
            except Exception as e:
                logger.error(f"Error cleaning up HTML crawler: {e}")
            finally:
                self.crawler = None
