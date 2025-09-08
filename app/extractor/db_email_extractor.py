import re
import json
from typing import List, Dict, Any
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

class DBEmailExtractor:
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.db_manager = DatabaseManager()
        
        # Email patterns
        self.email_patterns = [
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
            r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}",
        ]
        self.invalid_email = [r"noreply@", r"no-reply@", r"example\.com", r"@\d+\.\d+"]
    
    def _find_emails(self, text: str) -> List[str]:
        """Tìm emails từ text sử dụng regex patterns"""
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
    
    def extract_emails_from_html(self, html_content: str) -> List[str]:
        """Extract và validate emails từ HTML content"""
        emails = self._find_emails(html_content)
        valid_emails = [email for email in emails if self._valid_email(email)]
        return valid_emails
    
    def extract_from_db_batch(self, batch_size: int = 50) -> Dict[str, Any]:
        """Extract emails từ HTML records trong database"""
        # Get pending HTML records
        html_records = self.db_manager.get_pending_html(batch_size)
        
        if not html_records:
            return {
                'status': 'no_pending',
                'message': 'No pending HTML records found',
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
                # Extract emails from HTML
                emails = self.extract_emails_from_html(record['html_content'])
                
                # Determine email source based on URL
                email_source = 'website'
                if 'facebook.com' in record['url'].lower():
                    email_source = 'facebook'
                elif 'google.com' in record['url'].lower():
                    email_source = 'google'
                
                # Store extraction results
                self.db_manager.store_email_extraction(
                    html_storage_id=record['id'],
                    company_name=record['company_name'],
                    extracted_emails=emails,
                    email_source=email_source,
                    extraction_method='regex',
                    confidence_score=0.8 if emails else 0.0
                )
                
                # Update HTML record status
                self.db_manager.update_html_status(record['id'], 'processed')
                
                results['successful'] += 1
                results['details'].append({
                    'company': record['company_name'],
                    'url': record['url'],
                    'emails_found': len(emails),
                    'emails': emails,
                    'source': email_source
                })
                
                logger.info(f"Extracted {len(emails)} emails for {record['company_name']}")
                
            except Exception as e:
                logger.error(f"Failed to extract emails for {record['company_name']}: {e}")
                self.db_manager.update_html_status(record['id'], 'failed', retry_count=1)
                results['failed'] += 1
                results['details'].append({
                    'company': record['company_name'],
                    'url': record['url'],
                    'error': str(e)
                })
        
        return results
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get summary of email extraction results"""
        stats = self.db_manager.get_stats()
        
        # Get recent extraction results
        recent_results = self.db_manager.get_extraction_results()
        
        # Calculate additional metrics
        total_emails = 0
        companies_with_emails = 0
        
        for result in recent_results:
            emails = result['extracted_emails']
            if emails:
                total_emails += len(emails)
                companies_with_emails += 1
        
        return {
            **stats,
            'total_emails_extracted': total_emails,
            'companies_with_emails': companies_with_emails,
            'avg_emails_per_company': total_emails / max(companies_with_emails, 1)
        }
