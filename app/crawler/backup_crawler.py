import pandas as pd
import asyncio
import logging
from typing import List, Dict, Any
from app.extractor.email_extractor import EmailExtractor
from config import CrawlerConfig

logger = logging.getLogger(__name__)


class BackupCrawler:
    """Backup crawler để crawl lại các dòng có extracted_emails = N/A"""
    
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.email_extractor = EmailExtractor(self.config)
        
    def load_merged_file(self, file_path: str) -> pd.DataFrame:
        """Load file đã merge và filter các dòng có extracted_emails = N/A"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_path}")
            
            # Filter các dòng có extracted_emails = N/A
            na_rows = df[df['extracted_emails'] == 'N/A']
            logger.info(f"Found {len(na_rows)} rows with N/A emails")
            
            return na_rows
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {e}")
            return pd.DataFrame()
    
    async def deep_crawl_emails(self, row: pd.Series) -> Dict[str, Any]:
        """Deep crawl email với logic chặt chẽ hơn"""
        result = {
            'name': row['name'],
            'website': row.get('website', 'N/A'),
            'facebook': row.get('facebook', 'N/A'),
            'extracted_emails': 'N/A',
            'email_source': 'N/A'
        }
        
        try:
            # Deep crawl với multiple strategies
            emails = await self._deep_crawl_strategies(row)
            
            if emails:
                result['extracted_emails'] = '; '.join(emails)
                result['email_source'] = 'Deep Crawl'
                logger.info(f"Deep crawl success for {row['name']}: {emails}")
            else:
                logger.warning(f"Deep crawl failed for {row['name']}")
                
        except Exception as e:
            logger.error(f"Deep crawl error for {row['name']}: {e}")
            
        return result
    
    async def _deep_crawl_strategies(self, row: pd.Series) -> List[str]:
        """Multiple strategies để crawl email"""
        emails = []
        
        # Strategy 1: Website với multiple queries
        if row.get('website') and row['website'] != 'N/A':
            website_emails = await self._crawl_website_deep(row['website'])
            if website_emails:
                emails.extend(website_emails)
        
        # Strategy 2: Facebook với multiple queries
        if row.get('facebook') and row['facebook'] != 'N/A':
            facebook_emails = await self._crawl_facebook_deep(row['facebook'])
            if facebook_emails:
                emails.extend(facebook_emails)
        
        # Strategy 3: Google search với company name
        if not emails and row.get('name'):
            google_emails = await self._crawl_google_search(row['name'])
            if google_emails:
                emails.extend(google_emails)
        
        return list(set(emails))  # Remove duplicates
    
    async def _crawl_website_deep(self, website: str) -> List[str]:
        """Deep crawl website với multiple queries"""
        queries = [
            "Find all business contact emails in Contact, About, Team, or Footer sections",
            "Extract all email addresses from the entire page content",
            "Look for emails in Contact Us, Support, or Help sections",
            "Find emails in company information or business details"
        ]
        
        all_emails = []
        for query in queries:
            try:
                emails = await self.email_extractor._crawl_with_query(website, query)
                if emails:
                    all_emails.extend(emails)
            except Exception as e:
                logger.warning(f"Website query failed for {website}: {e}")
                continue
        
        return list(set(all_emails))
    
    async def _crawl_facebook_deep(self, facebook_url: str) -> List[str]:
        """Deep crawl Facebook với multiple queries"""
        queries = [
            "Find business contact emails in About section",
            "Extract emails from Contact Information or Business Details",
            "Look for emails in Posts or Comments",
            "Find emails in Page Info or Description"
        ]
        
        all_emails = []
        for query in queries:
            try:
                emails = await self.email_extractor._crawl_with_query(facebook_url, query)
                if emails:
                    all_emails.extend(emails)
            except Exception as e:
                logger.warning(f"Facebook query failed for {facebook_url}: {e}")
                continue
        
        return list(set(all_emails))
    
    async def _crawl_google_search(self, company_name: str) -> List[str]:
        """Crawl Google search results cho company name"""
        try:
            search_query = f"{company_name} contact email"
            google_url = f"https://www.google.com/search?q={search_query}"
            
            emails = await self.email_extractor._crawl_with_query(
                google_url, 
                "Find business contact emails in search results"
            )
            return emails or []
        except Exception as e:
            logger.warning(f"Google search failed for {company_name}: {e}")
            return []
    
    def update_merged_file(self, original_file: str, backup_results: List[Dict[str, Any]]) -> str:
        """Update file đã merge với kết quả backup crawl"""
        try:
            # Load original file
            df = pd.read_csv(original_file)
            logger.info(f"Loaded {len(df)} rows from {original_file}")
            
            # Update rows với backup results
            updated_count = 0
            for result in backup_results:
                mask = df['name'] == result['name']
                if mask.any():
                    old_email = df.loc[mask, 'extracted_emails'].iloc[0]
                    df.loc[mask, 'extracted_emails'] = result['extracted_emails']
                    df.loc[mask, 'email_source'] = result['email_source']
                    
                    if old_email == 'N/A' and result['extracted_emails'] != 'N/A':
                        updated_count += 1
                        logger.info(f"Updated {result['name']}: {old_email} → {result['extracted_emails']}")
            
            # Save updated file (ghi đè file gốc)
            df.to_csv(original_file, index=False)
            
            logger.info(f"Updated {updated_count} rows in original file: {original_file}")
            return original_file
            
        except Exception as e:
            logger.error(f"Error updating file: {e}")
            return original_file
