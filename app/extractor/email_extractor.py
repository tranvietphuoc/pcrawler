import re
import json
import asyncio
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from app.crawler.async_context_manager import get_context_manager
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

class EmailExtractor:
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.db_manager = DatabaseManager()
        
        # Use Async Context Manager for browser management
        self.context_manager = get_context_manager()
        self.crawler_id = f"EmailExtractor_{id(self)}"
        
        # Email patterns for fallback
        self.email_patterns = [
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
            r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}",
        ]
        self.invalid_email = [r"noreply@", r"no-reply@", r"example\.com", r"@\d+\.\d+"]

        # Load Crawl4ai config từ config
        crawl4ai_config = self.config.crawl4ai_config
        
        # Email-related keywords for scoring từ config
        self.email_keywords = crawl4ai_config.get('email_keywords', {
            'website': [
                "contact", "email", "mail", "lien he", "lienhe", "about", "gioi thieu",
                "info", "support", "help", "reach", "get in touch", "connect"
            ],
            'facebook': [
                "about", "contact", "email", "mail", "info", "business", "company",
                "lien he", "lienhe", "thong tin", "thongtin"
            ],
            'google': [
                "contact", "email", "mail", "about", "info", "business"
            ]
        })
        
        # Deep crawling strategy configuration từ config
        self.crawling_config = {
            'max_depth': crawl4ai_config.get('max_depth', 2),
            'max_pages': crawl4ai_config.get('max_pages', 25),
            'include_external': crawl4ai_config.get('include_external', False),
            'scorer_weight': crawl4ai_config.get('scorer_weight', 0.7)
        }
        
        logger.info(f"Loaded email keywords for scoring: {list(self.email_keywords.keys())}")
    
    def _find_emails_regex(self, text: str) -> List[str]:
        """Tìm emails từ text sử dụng regex patterns (fallback)"""
        emails = []
        for pattern in self.email_patterns:
            emails.extend(re.findall(pattern, text or ""))
        return list(set(email.strip() for email in emails if email))
    
    def _valid_email(self, email: str) -> bool:
        """Validate email dựa trên invalid patterns"""
        email_lower = email.lower()
        for pattern in self.invalid_email:
            if re.search(pattern, email_lower):
                return False
        return True

    async def extract_emails_with_best_first_crawling(self, html_content: str, url_type: str) -> List[str]:
        """Extract emails using BestFirstCrawlingStrategy với async_context_manager từ HTML content"""
        try:
            # Get appropriate keywords for URL type
            keywords = self.email_keywords.get(url_type, self.email_keywords['website'])
            
            # Create a scorer for email-related content
            scorer = KeywordRelevanceScorer(
                keywords=keywords,
                weight=self.crawling_config['scorer_weight']
            )
            
            # Configure the BestFirstCrawlingStrategy
            strategy = BestFirstCrawlingStrategy(
                max_depth=self.crawling_config['max_depth'],
                include_external=self.crawling_config['include_external'],
                url_scorer=scorer,
                max_pages=self.crawling_config['max_pages']
            )
            
            # Use Async Context Manager for browser management
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            viewport = {'width': 1920, 'height': 1080}
            
            async with self.context_manager.get_crawl4ai_crawler(self.crawler_id, user_agent, viewport) as crawler:
                # Use crawler.arun() with BestFirstCrawlingStrategy và raw HTML content
                result = await crawler.arun(
                    url="raw:" + html_content,  # Use raw HTML content từ database
                    crawling_strategy=strategy,
                    word_count_threshold=1,
                    bypass_cache=False,
                    wait_for="domcontentloaded",
                    delay_before_return_html=0.1
                )
                
                # Collect all extracted content from crawled pages
                all_emails = []
                
                # Extract emails from main page
                if result and result.extracted_content:
                    emails = self._find_emails_regex(result.extracted_content)
                    all_emails.extend(emails)
                
                # Extract emails from crawled pages
                if hasattr(result, 'crawled_pages') and result.crawled_pages:
                    for page in result.crawled_pages:
                        if page.get('extracted_content'):
                            emails = self._find_emails_regex(page['extracted_content'])
                            all_emails.extend(emails)
                
                # Filter valid emails and deduplicate
                valid_emails = list(set([email for email in all_emails if self._valid_email(email)]))
                
                logger.info(f"Extracted {len(valid_emails)} emails using BestFirstCrawlingStrategy for {url_type} (crawled {len(result.crawled_pages) if hasattr(result, 'crawled_pages') else 0} pages)")
                return valid_emails
            
        except Exception as e:
            logger.error(f"Failed to extract emails with BestFirstCrawlingStrategy: {e}")
            # Fallback to regex on HTML content
            try:
                async with self.context_manager.get_crawl4ai_crawler(self.crawler_id, user_agent, viewport) as crawler:
                    result = await crawler.arun(
                        url="raw:" + html_content,
                        word_count_threshold=1,
                        bypass_cache=False,
                        wait_for="domcontentloaded"
                    )
                    if result and result.extracted_content:
                        emails = self._find_emails_regex(result.extracted_content)
                        return [email for email in emails if self._valid_email(email)]
            except Exception as fallback_error:
                logger.error(f"Fallback extraction also failed: {fallback_error}")
            
            return []
    
    async def extract_emails_from_html(self, html_content: str, url_type: str) -> List[str]:
        """Extract emails từ HTML content using BestFirstCrawlingStrategy"""
        # Method 1: BestFirstCrawlingStrategy approach
        crawling_emails = await self.extract_emails_with_best_first_crawling(html_content, url_type)
        
        # Method 2: Simple regex fallback on HTML content
        regex_emails = self._find_emails_regex(html_content)
        regex_emails = [email for email in regex_emails if self._valid_email(email)]
        
        # Combine and deduplicate
        all_emails = list(set(crawling_emails + regex_emails))
        
        return all_emails
    
    def extract_from_db_batch(self, batch_size: int = 50) -> Dict[str, Any]:
        """Extract emails từ HTML records trong database"""
        # Get pending contact HTML records
        html_records = self.db_manager.get_pending_contact_html(batch_size)
        
        if not html_records:
            return {
                'status': 'no_pending',
                'message': 'No pending contact HTML records found',
                'processed': 0,
                'successful': 0,
                'failed': 0
            }
        
        results = {
            'status': 'completed',
            'processed': len(html_records),
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        for record in html_records:
            try:
                # Extract emails from HTML content using BestFirstCrawlingStrategy (async)
                emails = asyncio.run(self.extract_emails_from_html(record['html_content'], record['url_type']))
                
                # Store extraction results
                self.db_manager.store_email_extraction(
                    contact_html_id=record['id'],
                    company_name=record['company_name'],
                    extracted_emails=emails,
                    email_source=record['url_type'],
                    extraction_method='best_first_crawling',
                    confidence_score=0.9 if emails else 0.0
                )
                
                # Update HTML record status
                self.db_manager.update_contact_html_status(record['id'], 'processed')
                
                results['successful'] += 1
                results['details'].append({
                    'company': record['company_name'],
                    'url': record['url'],
                    'url_type': record['url_type'],
                    'emails_found': len(emails),
                    'emails': emails
                })
                
                logger.info(f"Extracted {len(emails)} emails for {record['company_name']} from {record['url_type']}")
                
            except Exception as e:
                logger.error(f"Failed to extract emails for {record['company_name']}: {e}")
                self.db_manager.update_contact_html_status(record['id'], 'failed', retry_count=1)
                results['failed'] += 1
                results['details'].append({
                    'company': record['company_name'],
                    'url': record['url'],
                    'url_type': record['url_type'],
                    'error': str(e)
                })
        
        return results
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get email extraction summary statistics"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Total extractions
                cursor.execute("SELECT COUNT(*) FROM email_extraction")
                total_extractions = cursor.fetchone()[0]
                
                # Successful extractions (with emails)
                cursor.execute("SELECT COUNT(*) FROM email_extraction WHERE extracted_emails != '[]' AND extracted_emails IS NOT NULL")
                successful_extractions = cursor.fetchone()[0]
                
                # Failed extractions
                failed_extractions = total_extractions - successful_extractions
                
                # Success rate
                success_rate = successful_extractions / max(total_extractions, 1)
                
                return {
                    'total_extractions': total_extractions,
                    'successful_extractions': successful_extractions,
                    'failed_extractions': failed_extractions,
                    'success_rate': success_rate
                }
                
        except Exception as e:
            logger.error(f"Failed to get extraction summary: {e}")
            return {
                'total_extractions': 0,
                'successful_extractions': 0,
                'failed_extractions': 0,
                'success_rate': 0.0
            }
