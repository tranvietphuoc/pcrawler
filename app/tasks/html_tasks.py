import asyncio
from celery import Celery
from app.crawler.html_crawler import HTMLCrawler
from app.crawler.detail_db_crawler import DetailDBCrawler
from app.extractor.crawl4ai_email_extractor import Crawl4AIEmailExtractor
from app.extractor.company_details_extractor import CompanyDetailsExtractor
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

# Import celery app
from app.tasks.celery_app import celery_app

@celery_app.task(name="detail.crawl_and_store", bind=True)
def crawl_detail_pages(self, companies: list, batch_size: int = 10):
    """
    Detail Crawler: Chỉ crawl detail pages và lưu HTML vào database (không extract)
    """
    try:
        config = CrawlerConfig()
        detail_crawler = DetailDBCrawler(config)
        
        # Tạo event loop mới
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            total_companies = len(companies)
            processed = 0
            successful = 0
            failed = 0
            
            # Process theo batch
            for i in range(0, total_companies, batch_size):
                batch = companies[i:i + batch_size]
                
                # Crawl detail pages batch (chỉ lưu HTML, không extract)
                batch_results = loop.run_until_complete(detail_crawler.crawl_batch(batch))
                
                processed += batch_results['total']
                successful += batch_results['successful']
                failed += batch_results['failed']
                
                # Update progress
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'current': processed,
                        'total': total_companies,
                        'successful': successful,
                        'failed': failed,
                        'status': f'Crawled detail pages batch {i//batch_size + 1}'
                    }
                )
                
                logger.info(f"Detail batch {i//batch_size + 1}: {batch_results['successful']}/{batch_results['total']} successful")
            
            # Cleanup
            detail_crawler.cleanup()
            
            return {
                'status': 'completed',
                'total_companies': total_companies,
                'processed': processed,
                'successful': successful,
                'failed': failed,
                'message': f'Detail pages crawling completed: {successful}/{total_companies} successful'
            }
            
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Detail pages crawling failed: {e}")
        return {
            'status': 'failed',
            'message': str(e),
            'total_companies': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0
        }

@celery_app.task(name="contact.crawl_from_details", bind=True)
def crawl_contact_pages_from_details(self, batch_size: int = 50):
    """
    Contact Crawler: Load company_details từ DB → crawl website/facebook (với auto close login) → lưu vào contact_html_storage
    """
    try:
        config = CrawlerConfig()
        html_crawler = HTMLCrawler(config)
        db_manager = DatabaseManager()
        
        # Tạo event loop mới
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Get company details từ DB
            company_details = db_manager.get_company_details_for_contact_crawl(batch_size)
            
            if not company_details:
                return {
                    'status': 'completed',
                    'message': 'No company details found for contact crawling',
                    'processed': 0,
                    'successful': 0,
                    'failed': 0
                }
            
            total_companies = len(company_details)
            processed = 0
            successful = 0
            failed = 0
            
            # Process theo batch
            for i in range(0, total_companies, batch_size):
                batch = company_details[i:i + batch_size]
                
                # Crawl contact pages batch
                batch_results = loop.run_until_complete(html_crawler.crawl_batch_from_details(batch))
                
                processed += batch_results['total']
                successful += batch_results['successful']
                failed += batch_results['failed']
                
                # Update progress
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'current': processed,
                        'total': total_companies,
                        'successful': successful,
                        'failed': failed,
                        'status': f'Crawled contact pages batch {i//batch_size + 1}'
                    }
                )
                
                logger.info(f"Contact batch {i//batch_size + 1}: {batch_results['successful']}/{batch_results['total']} successful")
            
            # Cleanup
            html_crawler.cleanup()
            
            return {
                'status': 'completed',
                'total_companies': total_companies,
                'processed': processed,
                'successful': successful,
                'failed': failed,
                'message': f'Contact pages crawling completed: {successful}/{total_companies} successful'
            }
            
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Contact pages crawling failed: {e}")
        return {
            'status': 'failed',
            'message': str(e),
            'total_companies': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0
        }

@celery_app.task(name="detail.extract_from_html", bind=True)
def extract_company_details(self, batch_size: int = 50):
    """
    Detail Extractor: Đọc HTML từ detail_html_storage, chia nhỏ tasks, extract XPath từ config, lưu vào company_details
    """
    try:
        config = CrawlerConfig()
        details_extractor = CompanyDetailsExtractor(config)
        
        # Extract company details from database
        results = details_extractor.extract_from_db_batch(batch_size)
        
        # Update progress
        self.update_state(
            state='PROGRESS',
            meta={
                'processed': results['processed'],
                'successful': results['successful'],
                'failed': results['failed'],
                'status': f'Extracted details from {results["processed"]} detail HTML records'
            }
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Company details extraction failed: {e}")
        return {
            'status': 'failed',
            'message': str(e),
            'processed': 0,
            'successful': 0,
            'failed': 0
        }

@celery_app.task(name="email.extract_from_contact", bind=True)
def extract_emails_from_contact(self, batch_size: int = 50):
    """
    Email Extractor: Load contact_html_storage (chỉ website/facebook) → crawl4ai extract emails → lưu vào email_extraction
    """
    try:
        config = CrawlerConfig()
        email_extractor = Crawl4AIEmailExtractor(config)
        
        # Extract emails from database
        results = email_extractor.extract_from_db_batch(batch_size)
        
        # Update progress
        self.update_state(
            state='PROGRESS',
            meta={
                'processed': results['processed'],
                'successful': results['successful'],
                'failed': results['failed'],
                'status': f'Extracted emails from {results["processed"]} contact HTML records'
            }
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Email extraction failed: {e}")
        return {
            'status': 'failed',
            'message': str(e),
            'processed': 0,
            'successful': 0,
            'failed': 0
        }

@celery_app.task(name="db.create_final_results")
def create_final_results():
    """
    Tạo bảng final_results: combine company_details + email_extraction → duplicate rows (max 5 emails)
    """
    try:
        db_manager = DatabaseManager()
        count = db_manager.create_final_results_with_duplication()
        
        return {
            'status': 'completed',
            'message': f'Created final results with {count} companies (duplicated for multiple emails)',
            'count': count
        }
        
    except Exception as e:
        logger.error(f"Failed to create final results: {e}")
        return {
            'status': 'failed',
            'message': str(e),
            'count': 0
        }

@celery_app.task(name="db.get_stats")
def get_database_stats():
    """
    Get database statistics
    """
    try:
        db_manager = DatabaseManager()
        stats = db_manager.get_stats()
        
        email_extractor = Crawl4AIEmailExtractor()
        summary = email_extractor.get_extraction_summary()
        
        return {
            'status': 'completed',
            'database_stats': stats,
            'extraction_summary': summary
        }
        
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}")
        return {
            'status': 'failed',
            'message': str(e)
        }
