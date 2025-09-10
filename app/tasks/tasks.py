import asyncio
import pandas as pd
import gc
import psutil
import time
import threading
from functools import lru_cache
from app.crawler.contact_crawler import ContactCrawler
from app.crawler.detail_crawler import DetailCrawler
from app.crawler.list_crawler import ListCrawler
from app.extractor.email_extractor import EmailExtractor
from app.extractor.company_details_extractor import CompanyDetailsExtractor
from app.database.db_manager import DatabaseManager
from app.utils.circuit_breaker import circuit_manager
from app.utils.health_monitor import health_monitor
from app.utils.error_handler import error_handler, fast_error_check
from config import CrawlerConfig
import logging

logger = logging.getLogger(__name__)

# Import celery app
from app.tasks.celery_app import celery_app

# Global event loop pool for better performance
_loop_pool = {}
_loop_lock = threading.Lock()

@lru_cache(maxsize=1)
def get_crawler_config():
    """Cached config instance"""
    return CrawlerConfig()

def _get_or_create_loop():
    """Get or create event loop for current thread (optimized)"""
    thread_id = threading.get_ident()
    
    with _loop_lock:
        if thread_id not in _loop_pool:
            try:
                # Try to get existing loop first
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except RuntimeError:
                # No event loop exists, create new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            _loop_pool[thread_id] = loop
        return _loop_pool[thread_id]

@celery_app.task(name="links.fetch_industry_links", bind=True)
def fetch_industry_links(self, base_url: str, industry_id: str, industry_name: str, pass_no: int = 1):
    """
    Fetch company links for a single industry (optimized with browser reuse and event loop pooling)
    """
    # Update task state
    self.update_state(state='PROGRESS', meta={'industry': industry_name, 'status': 'starting'})
    
    try:
        config = get_crawler_config()  # Use cached config
        list_crawler = ListCrawler(config)
        
        # Use pooled event loop for better performance
        loop = _get_or_create_loop()
        
        try:
            # Fetch links với optimized retry logic
            links = loop.run_until_complete(
                _fetch_links_with_circuit_breaker_async(list_crawler, base_url, industry_id, industry_name, pass_no)
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
            
            # Lưu checkpoint (sau khi hoàn thành chuẩn hoá)
            checkpoint_file = None
            if normalized:
                # Sanitize tên industry để tạo tên file hợp lệ
                import re
                import os
                safe_industry_name = re.sub(r'[^\w\s-]', '_', industry_name)  # Thay ký tự đặc biệt bằng _
                safe_industry_name = re.sub(r'[-\s]+', '_', safe_industry_name)  # Thay khoảng trắng và - bằng _
                safe_industry_name = safe_industry_name.strip('_')  # Bỏ _ ở đầu và cuối
            
                # Tạo thư mục data nếu chưa tồn tại
                os.makedirs('/app/data', exist_ok=True)
                checkpoint_file = f"/app/data/checkpoint_{safe_industry_name}_{pass_no}.json"
                
                try:
                    import json
                    with open(checkpoint_file, 'w') as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                    logger.info(f"Checkpoint saved: {checkpoint_file} ({len(normalized)} links)")
                except Exception as e:
                    logger.warning(f"Failed to save checkpoint: {e}")
            
            logger.info(f"Industry '{industry_name}' -> {len(normalized)} companies (pass {pass_no})")
            
            # Update task state to completed with checkpoint info
            self.update_state(state='SUCCESS', meta={
                'industry': industry_name, 
                'links_count': len(normalized),
                'checkpoint_file': checkpoint_file if normalized else None
            })
            
            # Return only metadata to avoid large result storage issues
            # The actual links are saved in checkpoint file
            result = {
                'industry': industry_name,
                'links_count': len(normalized),
                'checkpoint_file': checkpoint_file if normalized else None
            }
            logger.info(f"Returning result for '{industry_name}': {result}")
            return result
            
        finally:
            # Optimized cleanup để tránh "Task was destroyed but it is pending"
            try:
                # Cancel all pending tasks (optimized)
                pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
                if pending_tasks:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Cancelling {len(pending_tasks)} pending tasks...")
                    for task in pending_tasks:
                        task.cancel()
                    
                    # Wait for cancellation to complete (with timeout)
                    if pending_tasks:
                        try:
                            loop.run_until_complete(
                                asyncio.wait_for(
                                    asyncio.gather(*pending_tasks, return_exceptions=True),
                                    timeout=5.0  # 5 second timeout
                                )
                            )
                        except asyncio.TimeoutError:
                            logger.warning("Task cancellation timeout")
                
                # Cleanup crawler resources
                loop.run_until_complete(list_crawler.cleanup())
                
            except Exception as cleanup_error:
                logger.warning(f"Cleanup error: {cleanup_error}")
            # Note: Don't close loop in pool, reuse it
            
    except Exception as e:
        logger.error(f"Failed to fetch links for industry '{industry_name}': {e}")
        # Update task state to failed with proper exception info
        self.update_state(state='FAILURE', meta={
            'industry': industry_name, 
            'error_type': str(type(e).__name__),
            'error_message': str(e)[:500]  # Truncate long messages for JSON
        })
        # Don't re-raise to avoid serialization issues, just return error result
        return {
            'industry': industry_name,
            'links_count': 0,
            'checkpoint_file': None,
            'error': str(e)[:500]
        }

async def _fetch_links_with_circuit_breaker_async(list_crawler, base_url: str, industry_id: str, industry_name: str, pass_no: int = 1):
    """Async helper with circuit breaker and health monitoring integration"""
    # 1. Health check before starting
    logger.info(f"[{industry_name}] Starting health check...")
    health = await health_monitor.check_health(list_crawler.context_manager)
    if not health.is_healthy:
        logger.warning(f"[{industry_name}] Worker health issues detected: {health.issues}")
        await health_monitor.cleanup_if_needed(list_crawler.context_manager)
        logger.info(f"[{industry_name}] Health cleanup completed")
    else:
        logger.info(f"[{industry_name}] Worker health OK: {health.memory_usage_mb:.1f}MB, {health.cpu_percent:.1f}% CPU")
    
    # 2. Get circuit breaker for this industry
    breaker = circuit_manager.get_breaker(
        name=f"industry_links_{industry_id}",
        failure_threshold=3,  # Lower threshold for faster failover
        recovery_timeout=120,  # 2 minutes recovery time
        expected_exception=Exception
    )
    
    # 3. Check circuit breaker state
    breaker_state = breaker.get_state()
    logger.info(f"[{industry_name}] Circuit breaker state: {breaker_state['state']}, failures: {breaker_state['failure_count']}")
    
    # 4. Use circuit breaker to protect the main operation
    try:
        links = await breaker.call(
            _fetch_links_optimized_async,
            list_crawler, base_url, industry_id, industry_name, pass_no
        )
        logger.info(f"[{industry_name}] Circuit breaker protected operation completed successfully")
        return links
    except Exception as e:
        logger.error(f"[{industry_name}] Circuit breaker protected operation failed: {e}")
        # Check if circuit is now open
        final_state = breaker.get_state()
        if final_state['state'] == 'OPEN':
            logger.warning(f"[{industry_name}] Circuit breaker is now OPEN - will fail fast for {final_state['recovery_timeout']}s")
        raise e

async def _fetch_links_optimized_async(list_crawler, base_url: str, industry_id: str, industry_name: str, pass_no: int = 1):
    """Optimized async helper for link fetching with smart retry logic"""
    # Adaptive retries/timeouts per pass - tối ưu cho large industries
    if pass_no == 1:
        retries, timeout_s, delay_s = 2, 240, 2  # Giảm timeout xuống 4 phút, 2 retries
    else:
        retries, timeout_s, delay_s = 3, 480, 3  # Tăng timeout lên 8 phút cho pass 2+
    
    for attempt in range(retries + 1):
        try:
            # Progressive timeout: tăng timeout mỗi attempt
            current_timeout = timeout_s + (attempt * 60)  # Tăng 1 phút mỗi attempt
            logger.info(f"[{industry_name}] Attempt {attempt+1}/{retries+1} (pass {pass_no}) with timeout={current_timeout}s")
            
            links = await asyncio.wait_for(
                list_crawler.get_company_links_for_industry(base_url, industry_id, industry_name),
                timeout=current_timeout,
            )
            
            if links:
                logger.info(f"[{industry_name}] Success (pass {pass_no}) -> {len(links)} links")
                
                # Early termination for very large industries to prevent timeout
                if len(links) > 2000:
                    logger.warning(f"[{industry_name}] Very large industry ({len(links)} links) - consider splitting")
                
                return links
                
        except asyncio.TimeoutError:
            logger.warning(f"[{industry_name}] Timeout on attempt {attempt+1}/{retries+1} (pass {pass_no})")
        except Exception as e:
            # Optimized error handling
            error_info = fast_error_check(e)
            logger.warning(f"[{industry_name}] Error on attempt {attempt+1}/{retries+1} (pass {pass_no}): {error_info['type']} - {error_info['message']}")
            
            # Smart error handling - chỉ restart khi thật sự cần
            needs_restart = error_info['is_critical']
            
            if needs_restart:
                logger.warning(f"[{industry_name}] Critical error detected ({error_info['category']}), browser restart needed...")
            else:
                logger.info(f"[{industry_name}] Non-critical error ({error_info['category']}), retrying without restart...")
            
            # Restart browser if needed
            if needs_restart and attempt < retries:
                try:
                    await list_crawler.cleanup()
                    await asyncio.sleep(3)  # Shorter wait
                    # Browser will be recreated automatically on next call
                except Exception as cleanup_error:
                    logger.error(f"[{industry_name}] Cleanup failed: {cleanup_error}")
        
        # Random uniform delay before retry
        if attempt < retries:
            import random
            # Random delay: base_delay ± 50% jitter
            base_delay = delay_s * (attempt + 1)
            min_delay = base_delay * 0.5
            max_delay = base_delay * 1.5
            wait_time = random.uniform(min_delay, max_delay)
            logger.info(f"[{industry_name}] Waiting {wait_time:.1f}s before retry...")
            await asyncio.sleep(wait_time)
    
    logger.error(f"[{industry_name}] All attempts failed (pass {pass_no})")
    return []

async def _crawl_detail_pages_with_circuit_breaker_async(detail_crawler, companies: list, batch_size: int):
    """Async helper for detail crawling with circuit breaker and health monitoring"""
    total_companies = len(companies)
    processed = 0
    successful = 0
    failed = 0
    
    # 1. Health check before starting
    logger.info(f"Starting health check for detail crawling...")
    health = await health_monitor.check_health(detail_crawler.context_manager)
    if not health.is_healthy:
        logger.warning(f"Worker health issues detected: {health.issues}")
        await health_monitor.cleanup_if_needed(detail_crawler.context_manager)
        logger.info(f"Health cleanup completed")
    else:
        logger.info(f"Worker health OK: {health.memory_usage_mb:.1f}MB, {health.cpu_percent:.1f}% CPU")
    
    # 2. Get circuit breaker for detail crawling
    breaker = circuit_manager.get_breaker(
        name="detail_crawling",
        failure_threshold=5,  # Higher threshold for detail crawling
        recovery_timeout=180,  # 3 minutes recovery time
        expected_exception=Exception
    )
    
    # 3. Process batches with circuit breaker protection
    for i in range(0, total_companies, batch_size):
        batch = companies[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        try:
            # Use circuit breaker to protect batch crawling
            batch_results = await breaker.call(
                detail_crawler.crawl_batch,
                batch
            )
            
            processed += batch_results['total']
            successful += batch_results['successful']
            failed += batch_results['failed']
            
            logger.info(f"Detail batch {batch_num}: {batch_results['successful']}/{batch_results['total']} successful")
            
            # Health check after each batch
            health = await health_monitor.check_health(detail_crawler.context_manager)
            if not health.is_healthy:
                logger.warning(f"Health issues after batch {batch_num}: {health.issues}")
                await health_monitor.cleanup_if_needed(detail_crawler.context_manager)
            
        except Exception as e:
            logger.error(f"Detail batch {batch_num} failed: {e}")
            failed += len(batch)
            processed += len(batch)
            
            # Check circuit breaker state
            breaker_state = breaker.get_state()
            if breaker_state['state'] == 'OPEN':
                logger.warning(f"Circuit breaker is OPEN after batch {batch_num} - will fail fast")
                break  # Stop processing more batches
            
            continue
    
    # 4. Final cleanup
    await detail_crawler.cleanup()
    
    return {
        'status': 'completed',
        'total_companies': total_companies,
        'processed': processed,
        'successful': successful,
        'failed': failed,
        'message': f'Detail pages crawling completed: {successful}/{total_companies} successful'
    }

@celery_app.task(name="detail.crawl_and_store", bind=True)
def crawl_detail_pages(self, companies: list, batch_size: int = 10):
    """
    Detail Crawler: Chỉ crawl detail pages và lưu HTML vào database (không extract) - Optimized
    """
    try:
        config = get_crawler_config()  # Use cached config
        detail_crawler = DetailCrawler(config)
        
        # Use pooled event loop for better performance
        loop = _get_or_create_loop()
        
        try:
            # Use circuit breaker and health monitoring for detail crawling
            result = loop.run_until_complete(
                _crawl_detail_pages_with_circuit_breaker_async(detail_crawler, companies, batch_size)
            )
            return result
            
        finally:
            # Optimized cleanup để tránh "Task was destroyed but it is pending"
            try:
                # Cancel all pending tasks (optimized)
                pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
                if pending_tasks:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Cancelling {len(pending_tasks)} pending tasks...")
                    for task in pending_tasks:
                        task.cancel()
                    
                    # Wait for cancellation to complete (with timeout)
                    if pending_tasks:
                        try:
                            loop.run_until_complete(
                                asyncio.wait_for(
                                    asyncio.gather(*pending_tasks, return_exceptions=True),
                                    timeout=5.0  # 5 second timeout
                                )
                            )
                        except asyncio.TimeoutError:
                            logger.warning("Task cancellation timeout")
                
                # Cleanup crawler resources
                loop.run_until_complete(detail_crawler.cleanup())
                
            except Exception as cleanup_error:
                logger.warning(f"Cleanup error: {cleanup_error}")
            # Note: Don't close loop in pool, reuse it
            
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

@celery_app.task(name="health.check_worker_health", bind=True)
def check_worker_health(self):
    """
    Health check task for monitoring worker status
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            health_summary = health_monitor.get_health_summary()
            circuit_states = loop.run_until_complete(circuit_manager.get_all_states())
            
            logger.info(f"Health check completed: {health_summary}")
            
            return {
                "status": "success",
                "health": health_summary,
                "circuit_breakers": circuit_states,
                "timestamp": time.time()
            }
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time()
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
            # Proper cleanup để tránh "Task was destroyed but it is pending"
            try:
                # Cancel all pending tasks
                pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
                if pending_tasks:
                    logger.debug(f"Cancelling {len(pending_tasks)} pending tasks...")
                    for task in pending_tasks:
                        task.cancel()
                    
                    # Wait for cancellation to complete
                    if pending_tasks:
                        loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                
                # Cleanup crawler resources
                loop.run_until_complete(contact_crawler.cleanup())
                
            except Exception as cleanup_error:
                logger.warning(f"Cleanup error: {cleanup_error}")
            finally:
                # Close loop properly
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