import asyncio
import pandas as pd
import gc
import psutil
import time
from celery import Celery
from app.crawler.contact_crawler import ContactCrawler
from app.crawler.detail_crawler import DetailCrawler
from app.crawler.list_crawler import ListCrawler
from app.extractor.email_extractor import EmailExtractor
from app.extractor.company_details_extractor import CompanyDetailsExtractor
from app.database.db_manager import DatabaseManager
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

# Import celery app
from app.tasks.celery_app import celery_app

@celery_app.task(name="links.fetch_industry_links", bind=True)
def fetch_industry_links(self, base_url: str, industry_id: str, industry_name: str, pass_no: int = 1):
    """
    Fetch company links for a single industry (optimized with browser reuse)
    """
    try:
        config = CrawlerConfig()
        list_crawler = ListCrawler(config)
        
        # Tạo event loop mới
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Fetch links với optimized retry logic
            links = loop.run_until_complete(
                _fetch_links_optimized_async(list_crawler, base_url, industry_id, industry_name, pass_no)
            )
            
            # Chuẩn hoá dữ liệu
            normalized = []
            for item in links:
                if isinstance(item, str):
                    normalized.append({
                        'name': '',
                        'url': item,
                        'industry': industry_name,
                    })
                elif isinstance(item, dict):
                    item = {**item}
                    item['industry'] = industry_name
                    normalized.append(item)
            
            logger.info(f"Industry '{industry_name}' -> {len(normalized)} companies (pass {pass_no})")
            return normalized
            
        finally:
            # Chỉ cleanup khi thật sự cần (không cleanup mỗi task)
            # Browser sẽ được reuse cho task tiếp theo
            loop.close()
            
    except Exception as e:
        logger.error(f"Failed to fetch links for industry '{industry_name}': {e}")
        return []

async def _fetch_links_optimized_async(list_crawler, base_url: str, industry_id: str, industry_name: str, pass_no: int = 1):
    """Optimized async helper for link fetching with smart retry logic"""
    # Adaptive retries/timeouts per pass - tối ưu cho large industries
    if pass_no == 1:
        retries, timeout_s, delay_s = 2, 90, 2  # Tăng timeout, giảm retries
    else:
        retries, timeout_s, delay_s = 2, 120, 3  # Tăng timeout cho pass 2+
    
    for attempt in range(retries + 1):
        try:
            # Progressive timeout: tăng timeout mỗi attempt
            current_timeout = timeout_s + (attempt * 30)
            logger.info(f"[{industry_name}] Attempt {attempt+1}/{retries+1} (pass {pass_no}) with timeout={current_timeout}s")
            
            links = await asyncio.wait_for(
                list_crawler.get_company_links_for_industry(base_url, industry_id, industry_name),
                timeout=current_timeout,
            )
            
            if links:
                logger.info(f"[{industry_name}] Success (pass {pass_no}) -> {len(links)} links")
                return links
                
        except asyncio.TimeoutError:
            logger.warning(f"[{industry_name}] Timeout on attempt {attempt+1}/{retries+1} (pass {pass_no})")
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"[{industry_name}] Error on attempt {attempt+1}/{retries+1} (pass {pass_no}): {error_msg}")
            
            # Smart error handling - chỉ restart khi thật sự cần
            needs_restart = False
            
            # Critical errors that require browser restart
            if any(keyword in error_msg for keyword in [
                "Target page, context or browser has been closed",
                "TargetClosedError", 
                "Browser.new_context",
                "BrowserType.launch",
                "Protocol error",
                "Connection lost"
            ]):
                needs_restart = True
                logger.warning(f"[{industry_name}] Critical error detected, browser restart needed...")
            
            # Non-critical errors - just retry
            elif any(keyword in error_msg for keyword in [
                "Timeout",
                "Navigation timeout",
                "Network error",
                "Connection timeout"
            ]):
                logger.info(f"[{industry_name}] Network/timeout error, retrying without restart...")
            
            # Restart browser if needed
            if needs_restart and attempt < retries:
                try:
                    await list_crawler.cleanup()
                    await asyncio.sleep(3)  # Shorter wait
                    # Browser will be recreated automatically on next call
                except Exception as cleanup_error:
                    logger.error(f"[{industry_name}] Cleanup failed: {cleanup_error}")
        
        # Delay before retry (shorter for non-critical errors)
        if attempt < retries:
            wait_time = delay_s * (attempt + 1)
            logger.info(f"[{industry_name}] Waiting {wait_time}s before retry...")
            await asyncio.sleep(wait_time)
    
    logger.error(f"[{industry_name}] All attempts failed (pass {pass_no})")
    return []

@celery_app.task(name="detail.crawl_and_store", bind=True)
def crawl_detail_pages(self, companies: list, batch_size: int = 10):
    """
    Detail Crawler: Chỉ crawl detail pages và lưu HTML vào database (không extract)
    """
    try:
        config = CrawlerConfig()
        detail_crawler = DetailCrawler(config)
        
        # Tạo event loop mới
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Create fresh browser for this task to prevent context errors
            # Browser will be created automatically by context manager
            total_companies = len(companies)
            processed = 0
            successful = 0
            failed = 0
            
            # Process theo batch với error handling
            for i in range(0, total_companies, batch_size):
                batch = companies[i:i + batch_size]
                
                try:
                    # Memory monitoring
                    memory_before = psutil.Process().memory_info().rss / 1024 / 1024  # MB
                    
                    # Crawl detail pages batch (chỉ lưu HTML, không extract)
                    batch_results = loop.run_until_complete(detail_crawler.crawl_batch(batch))
                    
                    processed += batch_results['total']
                    successful += batch_results['successful']
                    failed += batch_results['failed']
                    
                    # Memory cleanup after each batch
                    memory_after = psutil.Process().memory_info().rss / 1024 / 1024  # MB
                    gc.collect()  # Force garbage collection
                    memory_after_gc = psutil.Process().memory_info().rss / 1024 / 1024  # MB
                    
                    # Update progress
                    self.update_state(
                        state='PROGRESS',
                        meta={
                            'current': processed,
                            'total': total_companies,
                            'successful': successful,
                            'failed': failed,
                            'memory_mb': round(memory_after_gc, 1),
                            'status': f'Crawled detail pages batch {i//batch_size + 1}'
                        }
                    )
                    
                    logger.info(f"Detail batch {i//batch_size + 1}: {batch_results['successful']}/{batch_results['total']} successful, Memory: {memory_before:.1f}MB → {memory_after_gc:.1f}MB")
                    
                    # Memory threshold check
                    if memory_after_gc > 1000:  # 1GB threshold
                        logger.warning(f"High memory usage: {memory_after_gc:.1f}MB, forcing cleanup")
                        loop.run_until_complete(detail_crawler.cleanup())
                        time.sleep(2)
                        # Browser will be created automatically by context manager
                    
                except Exception as batch_error:
                    logger.error(f"Detail batch {i//batch_size + 1} failed: {batch_error}")
                    failed += len(batch)
                    processed += len(batch)
                    
                    # Force cleanup on error
                    try:
                        loop.run_until_complete(detail_crawler.cleanup())
                        time.sleep(1)
                        # Browser will be created automatically by context manager
                    except:
                        pass
                    
                    # Continue with next batch instead of failing entire task
                    continue
            
            # Cleanup
            loop.run_until_complete(detail_crawler.cleanup())
            
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
        contact_crawler = ContactCrawler(config)
        db_manager = DatabaseManager()
        
        # Tạo event loop mới
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Create fresh browser for this task to prevent context errors
            # Browser will be created automatically by context manager
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
            
            # Process theo batch với error handling
            for i in range(0, total_companies, batch_size):
                batch = company_details[i:i + batch_size]
                
                try:
                    # Memory monitoring
                    memory_before = psutil.Process().memory_info().rss / 1024 / 1024  # MB
                    
                    # Crawl contact pages batch
                    batch_results = loop.run_until_complete(contact_crawler.crawl_batch_from_details(batch))
                    
                    processed += batch_results['total']
                    successful += batch_results['successful']
                    failed += batch_results['failed']
                    
                    # Memory cleanup after each batch
                    memory_after = psutil.Process().memory_info().rss / 1024 / 1024  # MB
                    gc.collect()  # Force garbage collection
                    memory_after_gc = psutil.Process().memory_info().rss / 1024 / 1024  # MB
                    
                    # Update progress
                    self.update_state(
                        state='PROGRESS',
                        meta={
                            'current': processed,
                            'total': total_companies,
                            'successful': successful,
                            'failed': failed,
                            'memory_mb': round(memory_after_gc, 1),
                            'status': f'Crawled contact pages batch {i//batch_size + 1}'
                        }
                    )
                    
                    logger.info(f"Contact batch {i//batch_size + 1}: {batch_results['successful']}/{batch_results['total']} successful, Memory: {memory_before:.1f}MB → {memory_after_gc:.1f}MB")
                    
                    # Memory threshold check
                    if memory_after_gc > 1000:  # 1GB threshold
                        logger.warning(f"High memory usage: {memory_after_gc:.1f}MB, forcing cleanup")
                        loop.run_until_complete(contact_crawler.cleanup())
                        time.sleep(2)
                        # Browser will be created automatically by context manager
                    
                except Exception as batch_error:
                    logger.error(f"Contact batch {i//batch_size + 1} failed: {batch_error}")
                    failed += len(batch)
                    processed += len(batch)
                    
                    # Force cleanup on error
                    try:
                        loop.run_until_complete(contact_crawler.cleanup())
                        time.sleep(1)
                        # Browser will be created automatically by context manager
                    except:
                        pass
                    
                    # Continue with next batch instead of failing entire task
                    continue
            
            # Cleanup
            loop.run_until_complete(contact_crawler.cleanup())
            
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
        email_extractor = EmailExtractor(config)
        
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
        
        email_extractor = EmailExtractor()
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


@celery_app.task(name="final.export", bind=True)
def export_final_csv(self):
    """
    Phase 5: Join các bảng và xuất CSV cuối cùng theo cột yêu cầu
    Columns: industry, company_name, company_url, address, phone, website, facebook,
             linkedin, tiktok, youtube, instagram, created_year, revenue, scale,
             extracted_email, email_source, confidence_score
    """
    try:
        config = CrawlerConfig()
        output_path = config.output_config.get("final_output", "data/final.csv")
        db = DatabaseManager()
        with db.get_connection() as conn:
            query = """
                SELECT 
                    d.industry AS industry,
                    cd.company_name,
                    cd.company_url,
                    cd.address,
                    cd.phone,
                    cd.website,
                    cd.facebook,
                    cd.linkedin,
                    cd.tiktok,
                    cd.youtube,
                    cd.instagram,
                    cd.created_year,
                    cd.revenue,
                    cd.scale,
                    COALESCE(e.extracted_emails, '[]') AS extracted_email,
                    e.email_source,
                    e.confidence_score
                FROM company_details cd
                JOIN detail_html_storage d ON cd.detail_html_id = d.id
                LEFT JOIN email_extraction e ON e.company_name = cd.company_name
                ORDER BY cd.company_name
            """
            df = pd.read_sql_query(query, conn)
            # explode emails if JSON array to one row per email
            def split_emails(val):
                try:
                    lst = pd.json.loads(val) if isinstance(val, str) and val.startswith('[') else None
                except Exception:
                    lst = None
                if lst is None:
                    return [val] if val and val != '[]' else []
                return lst
            # Prepare rows
            rows = []
            for _, r in df.iterrows():
                emails = split_emails(r['extracted_email'])
                if not emails:
                    rows.append({**r.to_dict(), 'extracted_email': 'N/A'})
                else:
                    for em in emails[:5]:
                        rows.append({**r.to_dict(), 'extracted_email': em})
            out_df = pd.DataFrame(rows)
            # Ensure columns order
            cols = [
                'industry','company_name','company_url','address','phone','website','facebook',
                'linkedin','tiktok','youtube','instagram','created_year','revenue','scale',
                'extracted_email','email_source','confidence_score'
            ]
            out_df = out_df.reindex(columns=cols)
            out_df.to_csv(output_path, index=False)
        return {
            'status': 'completed',
            'rows': len(out_df),
            'output': output_path
        }
    except Exception as e:
        logger.error(f"Final export failed: {e}")
        return {
            'status': 'failed',
            'message': str(e)
    }
