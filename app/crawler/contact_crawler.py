import asyncio
import random
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
from .base_crawler import BaseCrawler
import logging

logger = logging.getLogger(__name__)

class ContactCrawler(BaseCrawler):
    def __init__(self, config: CrawlerConfig = None):
        super().__init__(config)
        self.db_manager = DatabaseManager()
        self.max_requests_per_browser = 100  # Override for ContactCrawler - balance memory vs stability

    async def crawl_contact_page(self, url: str, company_name: str, url_type: str = 'website') -> bool:
        """Crawl contact page và lưu HTML vào database (contact_html_storage)"""
        if not url or url in ("N/A", ""):
            return False
            
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        # Use Async Context Manager for Crawl4AI crawler
        user_agent = await self._get_random_user_agent()
        viewport = await self._get_random_viewport()
        async with self.context_manager.get_crawl4ai_crawler(self.crawler_id, user_agent, viewport) as crawler:
            try:
                if url_type == "facebook":
                    # Facebook crawling với safe network handling
                    try:
                        result = await crawler.arun(
                            url=url,
                            init_script="""
                                // Auto close login popup ngay khi trang load
                                (function() {
                                    const closePopup = () => {
                                        // Tìm và click close button
                                        const closeButtons = document.querySelectorAll('[aria-label="Close"], [aria-label="Đóng"], .close, .x');
                                        for (let btn of closeButtons) {
                                            if (btn.offsetParent !== null) { // Visible
                                                btn.click();
                                                console.log('Closed Facebook popup');
                                                break;
                                            }
                                        }
                                        
                                        // Tìm và click "Not Now" button
                                        const notNowButtons = document.querySelectorAll('[aria-label="Not Now"], [aria-label="Không phải bây giờ"]');
                                        for (let btn of notNowButtons) {
                                            if (btn.offsetParent !== null) { // Visible
                                                btn.click();
                                                console.log('Clicked Not Now button');
                                                break;
                                            }
                                        }
                                    };
                                    
                                    // Chạy ngay khi DOM ready
                                    if (document.readyState === 'loading') {
                                        document.addEventListener('DOMContentLoaded', closePopup);
                                    } else {
                                        closePopup();
                                    }
                                    
                                    // Chạy lại sau 1 giây để catch popup muộn
                                    setTimeout(closePopup, 1000);
                                })();
                            """,
                            wait_for="networkidle"
                        )
                    except Exception as network_error:
                        error_str = str(network_error)
                        if "Target page, context or browser has been closed" in error_str or "TargetClosedError" in error_str:
                            logger.warning(f"Facebook networkidle failed due to browser closure, retrying with domcontentloaded: {network_error}")
                            raise  # Re-raise to trigger browser restart
                        else:
                            logger.warning(f"Facebook networkidle timeout, falling back to domcontentloaded: {network_error}")
                            result = await crawler.arun(
                                url=url,
                                init_script="""
                                    // Auto close login popup ngay khi trang load
                                    (function() {
                                        const closePopup = () => {
                                            // Tìm và click close button
                                            const closeButtons = document.querySelectorAll('[aria-label="Close"], [aria-label="Đóng"], .close, .x');
                                            for (let btn of closeButtons) {
                                                if (btn.offsetParent !== null) { // Visible
                                                    btn.click();
                                                    console.log('Closed Facebook popup');
                                                    break;
                                                }
                                            }
                                            
                                            // Tìm và click "Not Now" button
                                            const notNowButtons = document.querySelectorAll('[aria-label="Not Now"], [aria-label="Không phải bây giờ"]');
                                            for (let btn of notNowButtons) {
                                                if (btn.offsetParent !== null) { // Visible
                                                    btn.click();
                                                    console.log('Clicked Not Now button');
                                                    break;
                                                }
                                            }
                                        };
                                        
                                        // Chạy ngay khi DOM ready
                                        if (document.readyState === 'loading') {
                                            document.addEventListener('DOMContentLoaded', closePopup);
                                        } else {
                                            closePopup();
                                        }
                                        
                                        // Chạy lại sau 1 giây để catch popup muộn
                                        setTimeout(closePopup, 1000);
                                    })();
                                """,
                                wait_for="domcontentloaded"
                            )
                else:
                    # Website crawling - simple approach
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
                logger.error(f"Failed to crawl {url_type} for {company_name}: {url} - {e}")
                return False
    
    async def crawl_batch(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Crawl batch of companies with deduplication - skip already crawled URLs"""
        if not companies:
            return {'total': 0, 'successful': 0, 'failed': 0, 'skipped': 0}
        
        # Extract URLs for batch checking
        urls = []
        for company in companies:
            # Check both website and facebook URLs
            for url_field in ['website', 'facebook']:
                url = company.get(url_field, '')
                if url and url not in ("N/A", ""):
                    if not url.startswith(("http://", "https://")):
                        url = "https://" + url
                    urls.append(url)
        
        # Batch check which URLs already exist
        existing_urls = set()
        if urls:
            url_exists_map = self.db_manager.check_contact_urls_exist_batch(urls)
            existing_urls = {url for url, exists in url_exists_map.items() if exists}
        
        logger.info(f"Contact batch deduplication: {len(existing_urls)}/{len(urls)} URLs already exist, will skip")
        
        # Filter out existing URLs
        new_companies = []
        skipped_count = 0
        for company in companies:
            company_has_new_urls = False
            for url_field in ['website', 'facebook']:
                url = company.get(url_field, '')
                if url and url not in ("N/A", ""):
                    if not url.startswith(("http://", "https://")):
                        url = "https://" + url
                    if url in existing_urls:
                        skipped_count += 1
                        logger.debug(f"Skipping already crawled contact URL: {url}")
                        continue
                    else:
                        company_has_new_urls = True
            
            if company_has_new_urls:
                new_companies.append(company)
        
        logger.info(f"After contact deduplication: {len(new_companies)} companies with new URLs to crawl, {skipped_count} URLs skipped")
        
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
            company_name = company.get('name', '')
            company_url = company.get('url', '')
            website = company.get('website', '')
            facebook = company.get('facebook', '')
            
            # Crawl website
            if website and website not in ("N/A", ""):
                success = await self.crawl_contact_page(website, company_name, 'website')
                if success:
                    results['successful'] += 1
                    results['details'].append({
                        'company_name': company_name,
                        'url': website,
                        'url_type': 'website',
                        'status': 'success'
                    })
                else:
                    results['failed'] += 1
                    results['details'].append({
                        'company_name': company_name,
                        'url': website,
                        'url_type': 'website',
                        'status': 'failed'
                    })
            
            # Crawl Facebook
            if facebook and facebook not in ("N/A", ""):
                success = await self.crawl_contact_page(facebook, company_name, 'facebook')
                if success:
                    results['successful'] += 1
                    results['details'].append({
                        'company_name': company_name,
                        'url': facebook,
                        'url_type': 'facebook',
                        'status': 'success'
                    })
                else:
                    results['failed'] += 1
                    results['details'].append({
                        'company_name': company_name,
                        'url': facebook,
                        'url_type': 'facebook',
                        'status': 'failed'
                    })
        
        return results
    
    async def crawl_facebook_with_deep_pages(self, url: str, company_name: str) -> bool:
        """Crawl Facebook page với deep crawling (About, Contact, etc.)"""
        if not url or url in ("N/A", ""):
            return False
            
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        # Use Async Context Manager for Crawl4AI crawler
        user_agent = await self._get_random_user_agent()
        viewport = await self._get_random_viewport()
        async with self.context_manager.get_crawl4ai_crawler(self.crawler_id, user_agent, viewport) as crawler:
            try:
                # Crawl Facebook page với auto close login và deep crawling
                try:
                    result = await crawler.arun(
                        url=url,
                        init_script="""
                            // Auto close login popup và deep crawling ngay khi trang load
                            (function() {
                                const closePopup = () => {
                                    // Tìm và click close button
                                    const closeButtons = document.querySelectorAll('[aria-label="Close"], [aria-label="Đóng"], .close, .x');
                                    for (let btn of closeButtons) {
                                        if (btn.offsetParent !== null) { // Visible
                                            btn.click();
                                            console.log('Closed Facebook popup');
                                            break;
                                        }
                                    }
                                    
                                    // Tìm và click "Not Now" button
                                    const notNowButtons = document.querySelectorAll('[aria-label="Not Now"], [aria-label="Không phải bây giờ"]');
                                    for (let btn of notNowButtons) {
                                        if (btn.offsetParent !== null) { // Visible
                                            btn.click();
                                            console.log('Clicked Not Now button');
                                            break;
                                        }
                                    }
                                };
                                
                                const deepCrawl = () => {
                                    // Deep crawl: Navigate to About page
                                    const aboutLink = document.querySelector('a[href*="/about/"]');
                                    if (aboutLink) {
                                        aboutLink.click();
                                        console.log('Navigated to About page');
                                    }
                                    
                                    // Deep crawl: Navigate to Contact page
                                    setTimeout(() => {
                                        const contactLink = document.querySelector('a[href*="/contact/"]');
                                        if (contactLink) {
                                            contactLink.click();
                                            console.log('Navigated to Contact page');
                                        }
                                    }, 2000);
                                };
                                
                                // Chạy ngay khi DOM ready
                                if (document.readyState === 'loading') {
                                    document.addEventListener('DOMContentLoaded', () => {
                                        closePopup();
                                        setTimeout(deepCrawl, 1000);
                                    });
                                } else {
                                    closePopup();
                                    setTimeout(deepCrawl, 1000);
                                }
                                
                                // Chạy lại sau 1 giây để catch popup muộn
                                setTimeout(closePopup, 1000);
                            })();
                        """,
                        wait_for="networkidle"
                    )
                except Exception as network_error:
                    error_str = str(network_error)
                    if "Target page, context or browser has been closed" in error_str or "TargetClosedError" in error_str:
                        logger.warning(f"Facebook deep crawl networkidle failed due to browser closure, retrying with domcontentloaded: {network_error}")
                        raise  # Re-raise to trigger browser restart
                    else:
                        logger.warning(f"Facebook deep crawl networkidle timeout, falling back to domcontentloaded: {network_error}")
                        result = await crawler.arun(
                            url=url,
                            init_script="""
                                // Auto close login popup và deep crawling ngay khi trang load
                                (function() {
                                    const closePopup = () => {
                                        // Tìm và click close button
                                        const closeButtons = document.querySelectorAll('[aria-label="Close"], [aria-label="Đóng"], .close, .x');
                                        for (let btn of closeButtons) {
                                            if (btn.offsetParent !== null) { // Visible
                                                btn.click();
                                                console.log('Closed Facebook popup');
                                                break;
                                            }
                                        }
                                        
                                        // Tìm và click "Not Now" button
                                        const notNowButtons = document.querySelectorAll('[aria-label="Not Now"], [aria-label="Không phải bây giờ"]');
                                        for (let btn of notNowButtons) {
                                            if (btn.offsetParent !== null) { // Visible
                                                btn.click();
                                                console.log('Clicked Not Now button');
                                                break;
                                            }
                                        }
                                    };
                                    
                                    const deepCrawl = () => {
                                        // Deep crawl: Navigate to About page
                                        const aboutLink = document.querySelector('a[href*="/about/"]');
                                        if (aboutLink) {
                                            aboutLink.click();
                                            console.log('Navigated to About page');
                                        }
                                        
                                        // Deep crawl: Navigate to Contact page
                                        setTimeout(() => {
                                            const contactLink = document.querySelector('a[href*="/contact/"]');
                                            if (contactLink) {
                                                contactLink.click();
                                                console.log('Navigated to Contact page');
                                            }
                                        }, 2000);
                                    };
                                    
                                    // Chạy ngay khi DOM ready
                                    if (document.readyState === 'loading') {
                                        document.addEventListener('DOMContentLoaded', () => {
                                            closePopup();
                                            setTimeout(deepCrawl, 1000);
                                        });
                                    } else {
                                        closePopup();
                                        setTimeout(deepCrawl, 1000);
                                    }
                                    
                                    // Chạy lại sau 1 giây để catch popup muộn
                                    setTimeout(closePopup, 1000);
                                })();
                            """,
                            wait_for="domcontentloaded"
                        )
                
                html_content = getattr(result, "html", None) or getattr(result, "content", None) or str(result)
                
                if html_content and len(html_content) > 100:  # Valid HTML content
                    # Store to database
                    record_id = self.db_manager.store_contact_html(company_name, url, 'facebook', html_content)
                    logger.info(f"Stored Facebook deep crawl HTML for {company_name}: {url} (ID: {record_id})")
                    return True
                else:
                    logger.warning(f"Invalid HTML content for Facebook deep crawl {company_name}: {url}")
                    return False
                    
            except Exception as e:
                logger.error(f"Failed to deep crawl Facebook for {company_name}: {url} - {e}")
                return False
    
    async def crawl_batch_from_details(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Crawl contact pages từ company details (website + facebook)"""
        successful = 0
        failed = 0
        results = []
        
        for company in companies:
            company_name = company.get('company_name', '')
            website = company.get('website', '')
            facebook = company.get('facebook', '')
            
            # Random delay between companies
            await asyncio.sleep(random.uniform(2, 5))
            
            # Crawl website
            if website and website not in ("N/A", ""):
                success = await self.crawl_contact_page(website, company_name, 'website')
                if success:
                    successful += 1
                    results.append({
                        'company_name': company_name,
                        'url': website,
                        'url_type': 'website',
                        'status': 'success'
                    })
                else:
                    failed += 1
                    results.append({
                        'company_name': company_name,
                        'url': website,
                        'url_type': 'website',
                        'status': 'failed'
                    })
            
            # Crawl Facebook
            if facebook and facebook not in ("N/A", ""):
                success = await self.crawl_contact_page(facebook, company_name, 'facebook')
                if success:
                    successful += 1
                    results.append({
                        'company_name': company_name,
                        'url': facebook,
                        'url_type': 'facebook',
                        'status': 'success'
                    })
                else:
                    failed += 1
                    results.append({
                        'company_name': company_name,
                        'url': facebook,
                        'url_type': 'facebook',
                        'status': 'failed'
                    })
        
        return {
            'status': 'completed',
            'total': len(companies),
            'successful': successful,
            'failed': failed,
            'results': results
        }
