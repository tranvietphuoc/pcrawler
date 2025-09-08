import re
from typing import List, Dict, Any, Optional
from lxml import html as lxml_html
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

class CompanyDetailsExtractor:
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.db_manager = DatabaseManager()
        
        # Load XPath patterns từ config cũ (xpath section)
        xpath_config = self.config.xpath_config
        
        # Map XPath cũ thành patterns cho extraction (chỉ dùng XPath cũ, không fallback)
        self.xpath_patterns = {
            'name': [
                xpath_config.get('company_name', '')
            ],
            'address': [
                xpath_config.get('company_address', ''),
                xpath_config.get('company_address_fallback', '')
            ],
            'phone': [
                xpath_config.get('company_phone', '')
            ],
            'website': [
                xpath_config.get('company_website', '')
            ],
            'facebook': [
                xpath_config.get('social_media_container', '').replace('{platform}', 'facebook.com')
            ],
            'linkedin': [
                xpath_config.get('social_media_container', '').replace('{platform}', 'linkedin.com')
            ],
            'tiktok': [
                xpath_config.get('social_media_container', '').replace('{platform}', 'tiktok.com')
            ],
            'youtube': [
                xpath_config.get('social_media_container', '').replace('{platform}', 'youtube.com')
            ],
            'instagram': [
                xpath_config.get('social_media_container', '').replace('{platform}', 'instagram.com')
            ],
            'industry': [
                xpath_config.get('company_industry', '')
            ],
            'created_year': [
                xpath_config.get('company_created_year', '')
            ],
            'revenue': [
                xpath_config.get('company_revenue', '')
            ],
            'scale': [
                xpath_config.get('company_scale', '')
            ],
        }
        
        # Remove empty patterns
        for field, patterns in self.xpath_patterns.items():
            self.xpath_patterns[field] = [p for p in patterns if p.strip()]
        
        logger.info(f"Loaded XPath patterns from existing config: {list(self.xpath_patterns.keys())}")
    
    def extract_text_by_xpath(self, tree, xpath_patterns: List[str], field: Optional[str] = None) -> Optional[str]:
        """Extract text using lxml etree with real XPath."""
        for pattern in xpath_patterns:
            if not pattern.strip():
                continue
            try:
                nodes = tree.xpath(pattern)
                if not nodes:
                    continue
                first = nodes[0]
                # If XPath returns attribute/string
                if isinstance(first, (str, bytes)):
                    val = first.decode() if isinstance(first, bytes) else first
                    val = val.strip()
                    if val:
                        return val
                else:
                    # Element: decide how to extract
                    if field in ('website', 'facebook'):
                        href = first.get('href')
                        if href and href.strip():
                            return href.strip()
                    # Generic text
                    text_val = first.text_content().strip()
                    if text_val:
                        return text_val
            except Exception as e:
                logger.debug(f"XPath eval failed: {pattern} - {e}")
                continue
        return None
    
    def extract_company_details(self, html_content: str, company_name: str, company_url: str) -> Dict[str, Any]:
        """Extract company details from HTML content"""
        try:
            tree = lxml_html.fromstring(html_content)
        except Exception:
            tree = None
        details = {
            'company_name': company_name,
            'company_url': company_url,
            'industry': None,
            'address': None,
            'phone': None,
            'website': None,
            'facebook': None,
            'linkedin': None,
            'tiktok': None,
            'youtube': None,
            'instagram': None,
            'industry': None,
            'created_year': None,
            'revenue': None,
            'scale': None
        }
        
        # Extract each field
        for field, patterns in self.xpath_patterns.items():
            if field == 'name':
                continue  # Already have company_name
            if not tree:
                value = None
            else:
                value = self.extract_text_by_xpath(tree, patterns, field)
            if value:
                details[field] = value
                logger.debug(f"Extracted {field}: {value}")
        
        return details
    
    def extract_from_db_batch(self, batch_size: int = 50) -> Dict[str, Any]:
        """Extract company details từ HTML records trong database"""
        # Get pending detail HTML records
        html_records = self.db_manager.get_pending_detail_html(batch_size)
        
        if not html_records:
            return {
                'status': 'no_pending',
                'message': 'No pending detail HTML records found',
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
                # Extract company details from HTML
                details = self.extract_company_details(
                    record['html_content'], 
                    record['company_name'], 
                    record['company_url']
                )
                
                # Backup industry vào detail_html_storage nếu thiếu
                if details.get('industry'):
                    self.db_manager.update_detail_industry(record['id'], details['industry'])

                # Store company details (không lưu industry, industry nằm ở detail_html_storage)
                self.db_manager.store_company_details(
                    detail_html_id=record['id'],
                    company_name=details['company_name'],
                    company_url=details['company_url'],
                    address=details['address'],
                    phone=details['phone'],
                    website=details['website'],
                    facebook=details['facebook'],
                    linkedin=details['linkedin'],
                    tiktok=details['tiktok'],
                    youtube=details['youtube'],
                    instagram=details['instagram'],
                    created_year=details['created_year'],
                    revenue=details['revenue'],
                    scale=details['scale']
                )
                
                # Update HTML record status
                self.db_manager.update_detail_html_status(record['id'], 'processed')
                
                results['successful'] += 1
                results['details'].append({
                    'company': record['company_name'],
                    'url': record['company_url'],
                    'extracted_fields': {k: v for k, v in details.items() if v is not None}
                })
                
                logger.info(f"Extracted details for {record['company_name']}")
                
            except Exception as e:
                logger.error(f"Failed to extract details for {record['company_name']}: {e}")
                self.db_manager.update_detail_html_status(record['id'], 'failed', retry_count=1)
                results['failed'] += 1
                results['details'].append({
                    'company': record['company_name'],
                    'url': record['company_url'],
                    'error': str(e)
                })
        
        return results
