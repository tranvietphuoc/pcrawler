import sqlite3
import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging


logger = logging.getLogger(__name__)

# Removed global lock - using SQLite WAL mode + busy_timeout for concurrency

class DatabaseManager:
    def __init__(self, db_path: str = "data/crawler.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self) -> sqlite3.Connection:
        """Return SQLite connection with WAL and busy timeout to reduce locks."""
        conn = sqlite3.connect(self.db_path, timeout=60, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=30000;")  # Tăng từ 5s lên 30s
            conn.execute("PRAGMA cache_size=10000;")    # Tăng cache size
            conn.execute("PRAGMA temp_store=MEMORY;")   # Temp tables in memory
        except Exception:
            pass
        return conn
    
    def init_database(self):
        """Initialize database with schema"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Read and execute schema
        schema_path = Path(__file__).parent / "schema.sql"
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        with self.get_connection() as conn:
            conn.executescript(schema_sql)
            conn.commit()
        
        logger.info(f"Database initialized: {self.db_path}")
    
    def check_url_exists(self, company_url: str) -> bool:
        """Check if URL already exists in detail_html_storage"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM detail_html_storage 
                    WHERE company_url = ?
                """, (company_url,))
                count = cursor.fetchone()[0]
                return count > 0
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying check_url_exists for {company_url}")
                time.sleep(0.1)
                return self.check_url_exists(company_url)
            else:
                raise

    def check_urls_exist_batch(self, urls: List[str]) -> Dict[str, bool]:
        """Check multiple URLs at once for better performance"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Create placeholders for IN clause
                placeholders = ','.join(['?' for _ in urls])
                cursor.execute(f"""
                    SELECT company_url FROM detail_html_storage 
                    WHERE company_url IN ({placeholders})
                """, urls)
                existing_urls = {row[0] for row in cursor.fetchall()}
                return {url: url in existing_urls for url in urls}
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying check_urls_exist_batch")
                time.sleep(0.1)
                return self.check_urls_exist_batch(urls)
            else:
                raise

    def store_detail_html(self, company_name: str, company_url: str, html_content: str, industry: str = None) -> int:
        """Store detail page HTML content and return record ID"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO detail_html_storage (company_name, company_url, industry, html_content, status)
                    VALUES (?, ?, ?, ?, 'pending')
                """, (company_name, company_url, industry, html_content))
                record_id = cursor.lastrowid
                conn.commit()
                return record_id
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying store_detail_html for {company_name}")
                time.sleep(0.1)
                return self.store_detail_html(company_name, company_url, html_content, industry)
            else:
                raise
    
    def store_contact_html(self, company_name: str, url: str, url_type: str, html_content: str) -> int:
        """Store contact page HTML content and return record ID"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO contact_html_storage (company_name, url, url_type, html_content, status)
                    VALUES (?, ?, ?, ?, 'pending')
                """, (company_name, url, url_type, html_content))
                record_id = cursor.lastrowid
                conn.commit()
                return record_id
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying store_contact_html for {company_name}")
                time.sleep(0.1)
                return self.store_contact_html(company_name, url, url_type, html_content)
            else:
                raise
    
    def get_pending_detail_html(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get pending detail HTML records for company details extraction"""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, company_name, company_url, html_content, crawled_at
                FROM detail_html_storage 
                WHERE status = 'pending'
                ORDER BY crawled_at ASC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_pending_contact_html(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get pending contact HTML records for email extraction"""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, company_name, url, url_type, html_content, crawled_at
                FROM contact_html_storage 
                WHERE status = 'pending'
                ORDER BY crawled_at ASC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def update_detail_html_status(self, record_id: int, status: str, retry_count: int = 0):
        """Update detail HTML record status"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE detail_html_storage 
                SET status = ?, retry_count = ?
                WHERE id = ?
            """, (status, retry_count, record_id))
            conn.commit()
    
    def update_contact_html_status(self, record_id: int, status: str, retry_count: int = 0):
        """Update contact HTML record status"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE contact_html_storage 
                SET status = ?, retry_count = ?
                WHERE id = ?
            """, (status, retry_count, record_id))
            conn.commit()
    
    def store_company_details(self, detail_html_id: int, company_name: str, company_url: str,
                            address: str = None, phone: str = None, website: str = None,
                            facebook: str = None, linkedin: str = None, tiktok: str = None,
                            youtube: str = None, instagram: str = None,
                            description: str = None, created_year: str = None, revenue: str = None, 
                            scale: str = None):
        """Store company details extracted from detail page"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO company_details 
                    (detail_html_id, company_name, company_url, address, phone, website, facebook, 
                     linkedin, tiktok, youtube, instagram, created_year, revenue, scale)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (detail_html_id, company_name, company_url, address, phone, website, facebook, 
                      linkedin, tiktok, youtube, instagram, created_year, revenue, scale))
                conn.commit()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying store_company_details for {company_name}")
                time.sleep(0.1)
                return self.store_company_details(detail_html_id, company_name, company_url, address, 
                                                phone, website, facebook, linkedin, tiktok, youtube, 
                                                instagram, description, created_year, revenue, scale)
            else:
                raise

    def update_detail_industry(self, detail_html_id: int, industry: str):
        """Update industry in detail_html_storage if missing."""
        if not industry:
            return
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE detail_html_storage
                    SET industry = COALESCE(NULLIF(industry, ''), ?)
                    WHERE id = ?
                    """,
                    (industry, detail_html_id),
                )
                conn.commit()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying update_detail_industry for ID {detail_html_id}")
                import time
                time.sleep(0.1)
                return self.update_detail_industry(detail_html_id, industry)
            else:
                raise
    
    def store_email_extraction(self, contact_html_id: int, company_name: str, 
                             extracted_emails: List[str], email_source: str, 
                             extraction_method: str = "regex", confidence_score: float = 1.0):
        """Store email extraction results"""
        emails_json = json.dumps(extracted_emails)
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO email_extraction 
                    (contact_html_id, company_name, extracted_emails, email_source, 
                     extraction_method, confidence_score)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (contact_html_id, company_name, emails_json, email_source, 
                      extraction_method, confidence_score))
                conn.commit()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying store_email_extraction for {company_name}")
                time.sleep(0.1)
                return self.store_email_extraction(contact_html_id, company_name, extracted_emails, 
                                                 email_source, extraction_method, confidence_score)
            else:
                raise
    
    def get_extraction_results(self, company_name: str = None) -> List[Dict[str, Any]]:
        """Get email extraction results"""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if company_name:
                cursor.execute("""
                    SELECT e.*, h.url, h.url_type, h.crawled_at
                    FROM email_extraction e
                    JOIN contact_html_storage h ON e.contact_html_id = h.id
                    WHERE e.company_name = ?
                    ORDER BY e.processed_at DESC
                """, (company_name,))
            else:
                cursor.execute("""
                    SELECT e.*, h.url, h.url_type, h.crawled_at
                    FROM email_extraction e
                    JOIN contact_html_storage h ON e.contact_html_id = h.id
                    ORDER BY e.processed_at DESC
                """)
            
            results = []
            for row in cursor.fetchall():
                result = dict(row)
                result['extracted_emails'] = json.loads(result['extracted_emails'])
                results.append(result)
            
            return results
    
    # create_final_results removed; final export is done via pandas in task final.export
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Detail HTML storage stats
            cursor.execute("SELECT COUNT(*) FROM detail_html_storage")
            total_detail_html = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM detail_html_storage WHERE status = 'pending'")
            pending_detail_html = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM detail_html_storage WHERE status = 'processed'")
            processed_detail_html = cursor.fetchone()[0]
            
            # Contact HTML storage stats
            cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
            total_contact_html = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM contact_html_storage WHERE status = 'pending'")
            pending_contact_html = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM contact_html_storage WHERE status = 'processed'")
            processed_contact_html = cursor.fetchone()[0]
            
            # Company details stats
            cursor.execute("SELECT COUNT(*) FROM company_details")
            total_company_details = cursor.fetchone()[0]
            
            # Email extraction stats
            cursor.execute("SELECT COUNT(*) FROM email_extraction")
            total_extractions = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM email_extraction WHERE extracted_emails != '[]'")
            successful_extractions = cursor.fetchone()[0]
            
            return {
                'total_detail_html_records': total_detail_html,
                'pending_detail_html_records': pending_detail_html,
                'processed_detail_html_records': processed_detail_html,
                'total_contact_html_records': total_contact_html,
                'pending_contact_html_records': pending_contact_html,
                'processed_contact_html_records': processed_contact_html,
                'total_company_details': total_company_details,
                'total_extractions': total_extractions,
                'successful_extractions': successful_extractions,
                'extraction_success_rate': successful_extractions / max(total_extractions, 1)
            }
    
    def get_pending_detail_html(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get pending detail HTML records for processing"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, company_name, company_url, html_content, crawled_at
                FROM detail_html_storage 
                WHERE status = 'pending' 
                ORDER BY crawled_at ASC 
                LIMIT ?
            """, (limit,))
            
            records = []
            for row in cursor.fetchall():
                records.append({
                    'id': row['id'],
                    'company_name': row['company_name'],
                    'company_url': row['company_url'],
                    'html_content': row['html_content'],
                    'crawled_at': row['crawled_at']
                })
            
            return records
    
    def get_company_details_for_contact_crawl(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get company details để crawl contact pages (chỉ website và facebook)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, company_name, company_url, website, facebook
                FROM company_details 
                WHERE (website IS NOT NULL AND website != '') 
                   OR (facebook IS NOT NULL AND facebook != '')
                ORDER BY id ASC 
                LIMIT ?
            """, (limit,))
            
            records = []
            for row in cursor.fetchall():
                records.append({
                    'id': row['id'],
                    'company_name': row['company_name'],
                    'company_url': row['company_url'],
                    'website': row['website'],
                    'facebook': row['facebook']
                })
            
            return records
    
    def get_pending_contact_html(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get pending contact HTML records for email extraction"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, company_name, url, url_type, html_content, crawled_at
                FROM contact_html_storage 
                WHERE status = 'pending' 
                ORDER BY crawled_at ASC 
                LIMIT ?
            """, (limit,))
            
            records = []
            for row in cursor.fetchall():
                records.append({
                    'id': row['id'],
                    'company_name': row['company_name'],
                    'url': row['url'],
                    'url_type': row['url_type'],
                    'html_content': row['html_content'],
                    'crawled_at': row['crawled_at']
                })
            
            return records
    
    def create_final_results_with_duplication(self) -> int:
        """Create final results với logic duplicate rows cho multiple emails (max 5)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Clear existing final results
            cursor.execute("DELETE FROM final_results")
            
            # Get all company details
            cursor.execute("""
                SELECT company_name, company_url, address, phone, website, facebook, 
                       industry, description, linkedin, tiktok, youtube, instagram,
                       created_year, revenue, scale
                FROM company_details
                ORDER BY company_name
            """)
            company_details = cursor.fetchall()
            
            total_rows = 0
            
            for company in company_details:
                company_name = company[0]
                
                # Get emails for this company
                cursor.execute("""
                    SELECT extracted_emails, email_source
                    FROM email_extraction 
                    WHERE company_name = ?
                    ORDER BY processed_at ASC
                """, (company_name,))
                email_records = cursor.fetchall()
                
                if email_records:
                    # Limit to max 5 emails
                    emails_to_process = email_records[:5]
                    
                    for email_record in emails_to_process:
                        emails_str = email_record[0] or '[]'
                        email_source = email_record[1] or 'unknown'
                        
                        # Parse emails (JSON array)
                        try:
                            import json
                            emails = json.loads(emails_str) if emails_str != '[]' else []
                        except:
                            emails = [emails_str] if emails_str and emails_str != '[]' else []
                        
                        # Create one row per email
                        for email in emails:
                            if email and email.strip():
                                cursor.execute("""
                                    INSERT INTO final_results 
                                    (company_name, company_url, address, phone, website, facebook, 
                                     industry, description, extracted_emails, email_source, linkedin, tiktok, youtube, instagram,
                                     created_year, revenue, scale)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    company[0], company[1], company[2], company[3], company[4], company[5],
                                    company[6], company[7], email.strip(), email_source, company[8], 
                                    company[9], company[10], company[11], company[12], company[13], company[14]
                                ))
                                total_rows += 1
                else:
                    # No emails found, create one row without email
                    cursor.execute("""
                        INSERT INTO final_results 
                        (company_name, company_url, address, phone, website, facebook, 
                         industry, description, extracted_emails, email_source, linkedin, tiktok, youtube, instagram,
                         created_year, revenue, scale)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        company[0], company[1], company[2], company[3], company[4], company[5],
                        company[6], company[7], 'N/A', 'none', company[8], 
                        company[9], company[10], company[11], company[12], company[13], company[14]
                    ))
                    total_rows += 1
            
            conn.commit()
            return total_rows
