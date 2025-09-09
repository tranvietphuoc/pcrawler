import re
import json
import asyncio
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler, LLMExtractionStrategy
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

class EmailExtractor:
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.db_manager = DatabaseManager()
        
        # Email patterns for fallback
        self.email_patterns = [
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
            r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}",
        ]
        self.invalid_email = [r"noreply@", r"no-reply@", r"example\.com", r"@\d+\.\d+"]
        
        # Load Crawl4ai queries từ config
        crawl4ai_config = self.config.crawl4ai_config
        self.crawl4ai_queries = {
            'website': [
                crawl4ai_config.get('website_query', "Extract all email addresses from this page"),
                "Find contact email addresses",
                "Get email addresses for contact information",
                "Extract email addresses from contact section"
            ],
            'facebook': [
                crawl4ai_config.get('facebook_query', "Extract email addresses from this Facebook page"),
                "Find contact email addresses on this Facebook page",
                "Get email addresses from Facebook contact information"
            ],
            'google': [
                "Extract email addresses from search results",
                "Find email addresses in search results"
            ]
        }
        
        logger.info(f"Loaded Crawl4ai queries from config: {list(self.crawl4ai_queries.keys())}")
    
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
    
    async def extract_emails_with_crawl4ai_query(self, html_content: str, url_type: str) -> List[str]:
        """Extract emails using Crawl4ai query approach từ HTML content"""
        try:
            # Get appropriate queries for URL type
            queries = self.crawl4ai_queries.get(url_type, self.crawl4ai_queries['website'])
            
            crawler = AsyncWebCrawler()
            
            # Extract từ HTML content sử dụng Crawl4ai
            result = await crawler.extract(
                html_content=html_content,
                extraction_strategy=LLMExtractionStrategy(
                    provider="ollama/llama2",
                    api_token="ollama",
                    instruction=queries[0]  # Sử dụng query đầu tiên
                )
            )
            
            # Parse extracted content
            extracted_text = result.extracted_content or ""
            
            # Extract emails từ extracted text
            emails = self._find_emails_regex(extracted_text)
            
            # Filter valid emails
            valid_emails = [email for email in emails if self._valid_email(email)]
            
            logger.info(f"Extracted {len(valid_emails)} emails using Crawl4ai {url_type} query")
            return valid_emails
            
            # Note: crawler.close() is handled automatically by Async Context Manager
            
        except Exception as e:
            logger.error(f"Failed to extract emails with crawl4ai query: {e}")
            # Fallback to regex
            emails = self._find_emails_regex(html_content)
            return [email for email in emails if self._valid_email(email)]
    
    async def extract_emails_from_html(self, html_content: str, url_type: str) -> List[str]:
        """Extract emails từ HTML content using multiple methods"""
        # Method 1: Crawl4ai query approach
        crawl4ai_emails = await self.extract_emails_with_crawl4ai_query(html_content, url_type)
        
        # Method 2: Regex fallback
        regex_emails = self._find_emails_regex(html_content)
        regex_emails = [email for email in regex_emails if self._valid_email(email)]
        
        # Combine and deduplicate
        all_emails = list(set(crawl4ai_emails + regex_emails))
        
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
                # Extract emails from HTML (async)
                emails = asyncio.run(self.extract_emails_from_html(record['html_content'], record['url_type']))
                
                # Store extraction results
                self.db_manager.store_email_extraction(
                    contact_html_id=record['id'],
                    company_name=record['company_name'],
                    extracted_emails=emails,
                    email_source=record['url_type'],
                    extraction_method='crawl4ai_query',
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
