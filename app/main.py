import asyncio, argparse, logging
import os
import json
from typing import List, Dict, Any
from app.crawler.list_crawler import ListCrawler
from app.tasks.tasks import (
    fetch_industry_links as task_fetch_industry_links,
    crawl_detail_pages as task_crawl_detail_pages,
    extract_company_details as task_extract_company_details,
    crawl_contact_pages_from_details as task_crawl_contact_from_details,
    extract_emails_from_contact as task_extract_emails_from_contact,
    export_final_csv as task_export_final_csv,
)
from config import CrawlerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_checkpoint_completeness(links, industry_name):
    """
    Check if checkpoint is complete based on pagination and link quality analysis
    """
    if not links or len(links) == 0:
        return False, "Empty checkpoint"
    
    # 1. Check pagination completeness
    page_counts = {}
    for link in links:
        url = link.get('url', '') if isinstance(link, dict) else str(link)
        if 'page=' in url:
            try:
                # Extract page number from URL
                import re
                page_match = re.search(r'page=(\d+)', url)
                if page_match:
                    page_num = int(page_match.group(1))
                    page_counts[page_num] = page_counts.get(page_num, 0) + 1
            except:
                continue
    
    # Check for pagination gaps
    if page_counts:
        max_page = max(page_counts.keys())
        expected_pages = list(range(1, max_page + 1))
        missing_pages = [p for p in expected_pages if p not in page_counts]
        
        if missing_pages:
            return False, f"Missing pages: {missing_pages[:5]}{'...' if len(missing_pages) > 5 else ''}"
    
    # 2. Check link density (links per page)
    if page_counts:
        total_pages = len(page_counts)
        avg_links_per_page = len(links) / total_pages
        
        if avg_links_per_page < 5:  # Too few links per page
            return False, f"Low link density: {avg_links_per_page:.1f} links/page"
    
    # 3. Check for error patterns
    error_links = 0
    for link in links:
        url = link.get('url', '') if isinstance(link, dict) else str(link)
        if any(error in url.lower() for error in ['error', '404', 'not-found', 'timeout', 'failed']):
            error_links += 1
    
    if error_links > len(links) * 0.1:  # More than 10% error links
        return False, f"High error rate: {error_links}/{len(links)} error links"
    
    # 4. Check minimum link count for industry size
    if len(links) < 20:  # Very small industry
        return True, f"Small industry ({len(links)} links), likely complete"
    
    # 5. Check for reasonable link count
    if len(links) > 1000:  # Very large industry
        return True, f"Large industry ({len(links)} links), likely complete"
    
    # If all checks pass, consider complete
    return True, f"Complete checkpoint ({len(links)} links, {len(page_counts)} pages)"

async def run_phase1_links(config, base_url, batch_size):
    """Phase 1: Crawl links for all industries and save checkpoints"""
    logger.info("=" * 80)
    logger.info("PHASE 1: Crawling links for all industries...")
    logger.info("=" * 80)
    
    # Get industries
    list_c = ListCrawler(config=config)
    industries = await list_c.get_industries(base_url)
    logger.info(f"Found {len(industries)} industries")
    
    failed_industries: List[tuple] = []
    industry_link_counts: Dict[str, int] = {}
    detail_tasks = []
    total_links_processed = 0
    
    # Submit link fetching tasks in small waves to avoid overload
    wave_size = config.processing_config.get("industry_wave_size", 4)

    def iter_waves(items, size):
        for i in range(0, len(items), size):
            yield items[i:i+size]

    wave_index = 0
    for wave in iter_waves(industries, wave_size):
        wave_index += 1
        link_tasks = []
        logger.info(f"Submitting wave {wave_index} with {len(wave)} industries...")
        for idx, (ind_id, ind_name) in enumerate(wave, start=1):
            logger.info(f"[wave {wave_index} - {idx}/{len(wave)}] Submitting link fetching task for industry '{ind_name}'")
            task = task_fetch_industry_links.delay(base_url, ind_id, ind_name, 1)
            link_tasks.append((task, ind_id, ind_name))
        
        # Collect results for current wave and submit detail crawling tasks
        logger.info(f"Processing wave {wave_index} results...")
        
        # Process tasks in parallel with proper error handling
        completed_tasks = 0
        for idx, (task, ind_id, ind_name) in enumerate(link_tasks, start=1):
            try:
                # Wait for task completion and get result
                result = task.get(timeout=600)  # 10 minutes timeout per industry
                completed_tasks += 1
                logger.info(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> Task completed ({completed_tasks}/{len(link_tasks)})")
                
                # Check if task was successful by examining result
                if not result or not result.get('checkpoint_file'):
                    error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
                    logger.error(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> FAILED: {error_msg}; will retry later")
                    failed_industries.append((ind_id, ind_name))
                    continue
                
                # Get checkpoint file from result
                checkpoint_file = result.get('checkpoint_file')
                
                # Load links from checkpoint file
                try:
                    with open(checkpoint_file, 'r') as f:
                        links = json.load(f)
                    total_links = len(links)
                    logger.info(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> Loaded {total_links} links from checkpoint")
                    
                    # DEDUPLICATION: Remove duplicates from checkpoint
                    seen_urls = set()
                    deduplicated_links = []
                    duplicate_count = 0
                    
                    for link in links:
                        url = link.get('url', '') if isinstance(link, dict) else str(link)
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            deduplicated_links.append(link)
                        else:
                            duplicate_count += 1
                    
                    if duplicate_count > 0:
                        logger.info(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> Deduplication: {len(deduplicated_links)} unique links, {duplicate_count} duplicates removed")
                        links = deduplicated_links
                    
                    # DEDUPLICATION: Check which URLs already exist in database
                    from app.database.db_manager import DatabaseManager
                    db_manager = DatabaseManager()
                    
                    # Extract URLs for batch checking
                    urls = []
                    for link in links:
                        if isinstance(link, dict):
                            url = link.get('url', '')
                        else:
                            url = str(link)
                        if url and url not in ("N/A", ""):
                            if not url.startswith(("http://", "https://")):
                                url = "https://" + url
                            urls.append(url)
                    
                    # Batch check existing URLs
                    existing_urls = set()
                    if urls:
                        url_exists_map = db_manager.check_urls_exist_batch(urls)
                        existing_urls = {url for url, exists in url_exists_map.items() if exists}
                    
                    # Filter out existing URLs
                    new_links = []
                    skipped_count = 0
                    for link in links:
                        if isinstance(link, dict):
                            url = link.get('url', '')
                        else:
                            url = str(link)
                        
                        if url and url not in ("N/A", ""):
                            if not url.startswith(("http://", "https://")):
                                url = "https://" + url
                            if url in existing_urls:
                                skipped_count += 1
                                continue
                        new_links.append(link)
                    
                    logger.info(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> Deduplication: {len(new_links)} new links, {skipped_count} skipped")
                    
                    # Submit detail crawling tasks only for new links
                    if new_links:
                        logger.info(f"Submitting detail crawling tasks for industry '{ind_name}' ({len(new_links)} new companies) in batches...")
                        batch_count = 0
                        for i in range(0, len(new_links), batch_size):
                            batch = new_links[i:i+batch_size]
                            batch_count += 1
                            task = task_crawl_detail_pages.delay(batch, batch_size)
                            detail_tasks.append(task)
                            if batch_count % 10 == 0:  # Log progress every 10 batches
                                logger.info(f"[wave {wave_index}] Submitted {batch_count} batches for industry '{ind_name}'...")
                    
                    total_links_processed += len(new_links)
                    industry_link_counts[ind_name] = len(new_links)
                    
                    # Clear links from memory
                    del links, new_links
                    
                except Exception as e:
                    logger.error(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> Failed to load checkpoint: {e}")
                    failed_industries.append((ind_id, ind_name))
                    
            except Exception as e:
                logger.error(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> FAILED: {e}")
                failed_industries.append((ind_id, ind_name))
        
        logger.info(f"Wave {wave_index} completed: {completed_tasks}/{len(link_tasks)} tasks successful")

    logger.info(f"Total links processed: {total_links_processed} companies across {len(industries)} industries")
    
    # Retry failed industries with completeness check
    if failed_industries:
        logger.info(f"Checking {len(failed_industries)} failed industries for completeness...")
        retry_tasks = []
        skipped_industries = []
        
        # Check each failed industry for existing checkpoint and completeness
        for ind_id, ind_name in failed_industries:
            # Check if checkpoint already exists
            import re
            import os
            safe_industry_name = re.sub(r'[^\w\s-]', '_', ind_name)
            safe_industry_name = re.sub(r'[-\s]+', '_', safe_industry_name)
            safe_industry_name = safe_industry_name.strip('_')
            
            checkpoint_file = f"/app/data/checkpoint_{safe_industry_name}_1.json"
            
            if os.path.exists(checkpoint_file):
                try:
                    with open(checkpoint_file, 'r') as f:
                        existing_links = json.load(f)
                    
                    if existing_links and len(existing_links) > 0:
                        # COMPLETENESS CHECK: Analyze pagination and link quality
                        is_complete, completeness_reason = check_checkpoint_completeness(existing_links, ind_name)
                        
                        if is_complete:
                            logger.info(f"Industry '{ind_name}' appears complete ({len(existing_links)} links) - {completeness_reason} - SKIPPING retry")
                            skipped_industries.append((ind_id, ind_name, checkpoint_file, existing_links))
                            continue
                        else:
                            logger.info(f"Industry '{ind_name}' incomplete: {completeness_reason} - will retry")
                    else:
                        logger.info(f"Industry '{ind_name}' has empty checkpoint - will retry")
                except Exception as e:
                    logger.warning(f"Industry '{ind_name}' checkpoint corrupted: {e} - will retry")
            else:
                logger.info(f"Industry '{ind_name}' has no checkpoint - will retry")
            
            # Submit retry task only if no valid complete checkpoint exists
            logger.info(f"Submitting retry task for industry '{ind_name}'...")
            retry_task = task_fetch_industry_links.delay(base_url, ind_id, ind_name, 2)
            retry_tasks.append((retry_task, ind_id, ind_name))
        
        # Process skipped industries (complete checkpoints)
        if skipped_industries:
            logger.info(f"Processing {len(skipped_industries)} industries with complete checkpoints...")
            for ind_id, ind_name, checkpoint_file, existing_links in skipped_industries:
                try:
                    # DEDUPLICATION: Remove duplicates from existing checkpoint
                    seen_urls = set()
                    deduplicated_links = []
                    duplicate_count = 0
                    
                    for link in existing_links:
                        url = link.get('url', '') if isinstance(link, dict) else str(link)
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            deduplicated_links.append(link)
                        else:
                            duplicate_count += 1
                    
                    if duplicate_count > 0:
                        logger.info(f"Existing checkpoint deduplication: '{ind_name}' -> {len(deduplicated_links)} unique links, {duplicate_count} duplicates removed")
                        existing_links = deduplicated_links
                    
                    # DEDUPLICATION: Check which URLs already exist in database
                    from app.database.db_manager import DatabaseManager
                    db_manager = DatabaseManager()
                    
                    # Extract URLs for batch checking
                    urls = []
                    for link in existing_links:
                        if isinstance(link, dict):
                            url = link.get('url', '')
                        else:
                            url = str(link)
                        if url and url not in ("N/A", ""):
                            if not url.startswith(("http://", "https://")):
                                url = "https://" + url
                            urls.append(url)
                    
                    # Batch check existing URLs
                    existing_urls = set()
                    if urls:
                        url_exists_map = db_manager.check_urls_exist_batch(urls)
                        existing_urls = {url for url, exists in url_exists_map.items() if exists}
                    
                    # Filter out existing URLs
                    new_links = []
                    skipped_count = 0
                    for link in existing_links:
                        if isinstance(link, dict):
                            url = link.get('url', '')
                        else:
                            url = str(link)
                        
                        if url and url not in ("N/A", ""):
                            if not url.startswith(("http://", "https://")):
                                url = "https://" + url
                            if url in existing_urls:
                                skipped_count += 1
                                continue
                        new_links.append(link)
                    
                    logger.info(f"Existing checkpoint deduplication: '{ind_name}' -> {len(new_links)} new links, {skipped_count} skipped")
                    
                    # Submit detail tasks only for new links
                    if new_links:
                        for i in range(0, len(new_links), batch_size):
                            batch = new_links[i:i+batch_size]
                            task = task_crawl_detail_pages.delay(batch, batch_size)
                            detail_tasks.append(task)
                    
                    total_links_processed += len(new_links)
                    industry_link_counts[ind_name] = len(new_links)
                    
                except Exception as e:
                    logger.error(f"Failed to process existing checkpoint for industry '{ind_name}': {e}")
        
        # Wait a bit for tasks to be picked up by workers
        import time
        logger.info("Waiting 10 seconds for retry tasks to be picked up by workers...")
        time.sleep(10)
        
        # Wait for all retry tasks to complete with proper async handling
        logger.info(f"Waiting for {len(retry_tasks)} retry tasks to complete...")
        completed_retries = 0
        for retry_task, ind_id, ind_name in retry_tasks:
            try:
                logger.info(f"Waiting for retry task completion: '{ind_name}'...")
                # Use longer timeout and proper exception handling
                result = retry_task.get(timeout=7200)  # 2 hours timeout
                completed_retries += 1
                logger.info(f"Retry task completed: '{ind_name}' ({completed_retries}/{len(retry_tasks)})")
                
                if result and result.get('checkpoint_file'):
                    checkpoint_file = result.get('checkpoint_file')
                    with open(checkpoint_file, 'r') as f:
                        links = json.load(f)
                    total_links = len(links)
                    logger.info(f"Retry successful: '{ind_name}' -> {total_links} links")
                    
                    # DEDUPLICATION: Remove duplicates from retry checkpoint
                    seen_urls = set()
                    deduplicated_links = []
                    duplicate_count = 0
                    
                    for link in links:
                        url = link.get('url', '') if isinstance(link, dict) else str(link)
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            deduplicated_links.append(link)
                        else:
                            duplicate_count += 1
                    
                    if duplicate_count > 0:
                        logger.info(f"Retry deduplication: '{ind_name}' -> {len(deduplicated_links)} unique links, {duplicate_count} duplicates removed")
                        links = deduplicated_links
                    
                    # DEDUPLICATION: Check which URLs already exist in database
                    from app.database.db_manager import DatabaseManager
                    db_manager = DatabaseManager()
                    
                    # Extract URLs for batch checking
                    urls = []
                    for link in links:
                        if isinstance(link, dict):
                            url = link.get('url', '')
                        else:
                            url = str(link)
                        if url and url not in ("N/A", ""):
                            if not url.startswith(("http://", "https://")):
                                url = "https://" + url
                            urls.append(url)
                    
                    # Batch check existing URLs
                    existing_urls = set()
                    if urls:
                        url_exists_map = db_manager.check_urls_exist_batch(urls)
                        existing_urls = {url for url, exists in url_exists_map.items() if exists}
                    
                    # Filter out existing URLs
                    new_links = []
                    skipped_count = 0
                    for link in links:
                        if isinstance(link, dict):
                            url = link.get('url', '')
                        else:
                            url = str(link)
                        
                        if url and url not in ("N/A", ""):
                            if not url.startswith(("http://", "https://")):
                                url = "https://" + url
                            if url in existing_urls:
                                skipped_count += 1
                                continue
                        new_links.append(link)
                    
                    logger.info(f"Retry deduplication: '{ind_name}' -> {len(new_links)} new links, {skipped_count} skipped")
                    
                    # Submit detail tasks only for new links
                    if new_links:
                        for i in range(0, len(new_links), batch_size):
                            batch = new_links[i:i+batch_size]
                            task = task_crawl_detail_pages.delay(batch, batch_size)
                            detail_tasks.append(task)
                    
                    total_links_processed += len(new_links)
                    industry_link_counts[ind_name] = len(new_links)
                    del links, new_links
                else:
                    error_msg = result.get('error', 'No checkpoint file') if result else 'No result returned'
                    logger.error(f"Retry failed for industry '{ind_name}': {error_msg}")
                    
            except Exception as e:
                logger.error(f"Retry failed for industry '{ind_name}': {e}")
        
        logger.info(f"Retry phase completed: {completed_retries}/{len(retry_tasks)} tasks processed")
    
    return {
        'failed_industries': failed_industries,
        'industry_link_counts': industry_link_counts,
        'detail_tasks': detail_tasks,
        'total_links_processed': total_links_processed
    }

async def run_phase2_details(detail_tasks):
    """Phase 2: Wait for all detail crawling tasks to complete"""
    logger.info("=" * 80)
    logger.info("PHASE 2: Waiting for detail crawling tasks to complete...")
    logger.info("=" * 80)
    
    if not detail_tasks:
        logger.info("No detail tasks to process")
        return
    
    logger.info(f"Waiting for {len(detail_tasks)} detail crawling tasks to complete...")
    completed_details = 0
    failed_details = 0
    
    for i, task in enumerate(detail_tasks, 1):
        try:
            result = task.get(timeout=3600)  # 1 hour timeout per batch
            completed_details += 1
            if i % 10 == 0 or i == len(detail_tasks):
                logger.info(f"Detail crawling progress: {i}/{len(detail_tasks)} tasks completed")
        except Exception as e:
            failed_details += 1
            logger.error(f"Detail crawling task {i} failed: {e}")
    
    logger.info(f"Detail crawling completed: {completed_details} successful, {failed_details} failed")

async def run_phase3_extract_details(batch_size):
    """Phase 3: Extract company details from detail_html_storage"""
    logger.info("=" * 80)
    logger.info("PHASE 3: Extracting company details from detail_html_storage...")
    logger.info("=" * 80)
    
    # Check pending records in detail_html_storage
    from app.database.db_manager import DatabaseManager
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM detail_html_storage WHERE status = 'pending'")
        pending_details = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM company_details")
        existing_companies = cursor.fetchone()[0]
    
    logger.info(f"Pending detail records: {pending_details}")
    logger.info(f"Existing company details: {existing_companies}")
    
    if pending_details > 0:
        logger.info(f"Processing {pending_details} pending detail records in batches of {batch_size}")
        total_processed = 0
        total_successful = 0
        total_failed = 0
        
        while True:
            # Submit details extraction task
            details_task = task_extract_company_details.delay(batch_size)
            logger.info(f"Details extraction task submitted (batch {total_processed//batch_size + 1})")
            
            try:
                result = details_task.get(timeout=3600)  # 1 hour timeout
                total_processed += result.get('processed', 0)
                total_successful += result.get('successful', 0)
                total_failed += result.get('failed', 0)
                
                logger.info(f"Batch completed: {result}")
                
                # Check if no more pending records
                if result.get('status') == 'no_pending':
                    logger.info("No more pending detail records")
                    break
                    
            except Exception as e:
                logger.error(f"Details extraction failed: {e}")
                break
        
        logger.info(f"Details extraction summary: {total_processed} processed, {total_successful} successful, {total_failed} failed")
    else:
        logger.info("No pending detail records found for extraction")

async def run_phase4_contacts(batch_size):
    """Phase 4: Crawl contact pages from company_details"""
    logger.info("=" * 80)
    logger.info("PHASE 4: Crawling contact pages from company_details...")
    logger.info("=" * 80)
    
    # Check company details records
    from app.database.db_manager import DatabaseManager
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM company_details WHERE (website IS NOT NULL AND website != '') OR (facebook IS NOT NULL AND facebook != '')")
        companies_with_contacts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
        existing_contacts = cursor.fetchone()[0]
    
    logger.info(f"Companies with contact info: {companies_with_contacts}")
    logger.info(f"Existing contact records: {existing_contacts}")
    
    if companies_with_contacts > 0:
        logger.info(f"Processing {companies_with_contacts} companies with contact info in batches of {batch_size}")
        total_processed = 0
        total_successful = 0
        total_failed = 0
        
        while True:
            # Submit contact crawling task
            contact_task = task_crawl_contact_from_details.delay(batch_size)
            logger.info(f"Contact crawling task submitted (batch {total_processed//batch_size + 1})")
            
            try:
                result = contact_task.get(timeout=7200)  # 2 hours timeout
                total_processed += result.get('processed', 0)
                total_successful += result.get('successful', 0)
                total_failed += result.get('failed', 0)
                
                logger.info(f"Batch completed: {result}")
                
                # Check if no more records to process
                if result.get('status') == 'no_pending':
                    logger.info("No more companies to process for contact crawling")
                    break
                    
            except Exception as e:
                logger.error(f"Contact crawling failed: {e}")
                break
        
        logger.info(f"Contact crawling summary: {total_processed} processed, {total_successful} successful, {total_failed} failed")
    else:
        logger.info("No companies with contact info found for crawling")

async def run_phase5_extract_emails(batch_size):
    """Phase 5: Extract emails from contact_html_storage"""
    logger.info("=" * 80)
    logger.info("PHASE 5: Extracting emails from contact_html_storage...")
    logger.info("=" * 80)
    
    # Check pending contact records
    from app.database.db_manager import DatabaseManager
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM contact_html_storage WHERE status = 'pending'")
        pending_contacts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM email_extraction")
        existing_emails = cursor.fetchone()[0]
    
    logger.info(f"Pending contact records: {pending_contacts}")
    logger.info(f"Existing email extractions: {existing_emails}")
    
    if pending_contacts > 0:
        logger.info(f"Processing {pending_contacts} pending contact records in batches of {batch_size}")
        total_processed = 0
        total_successful = 0
        total_failed = 0
        
        while True:
            # Submit emails extraction task
            emails_task = task_extract_emails_from_contact.delay(batch_size)
            logger.info(f"Emails extraction task submitted (batch {total_processed//batch_size + 1})")
            
            try:
                result = emails_task.get(timeout=3600)  # 1 hour timeout
                total_processed += result.get('processed', 0)
                total_successful += result.get('successful', 0)
                total_failed += result.get('failed', 0)
                
                logger.info(f"Batch completed: {result}")
                
                # Check if no more pending records
                if result.get('status') == 'no_pending':
                    logger.info("No more pending contact records")
                    break
                    
            except Exception as e:
                logger.error(f"Emails extraction failed: {e}")
                break
        
        logger.info(f"Emails extraction summary: {total_processed} processed, {total_successful} successful, {total_failed} failed")
    else:
        logger.info("No pending contact records found for email extraction")

async def run_phase6_export():
    """Phase 6: Export final CSV"""
    logger.info("=" * 80)
    logger.info("PHASE 6: Exporting final CSV...")
    logger.info("=" * 80)
    
    # Submit export task
    export_task = task_export_final_csv.delay()
    logger.info("Export task submitted")
    
    # Wait for completion
    try:
        result = export_task.get(timeout=1800)  # 30 minutes timeout
        if result:
            logger.info(f"Export completed: {result}")
        else:
            logger.warning("Export task returned no result")
    except Exception as e:
        logger.error(f"Export failed: {e}")

async def run(config_name: str = "default", base_url: str = None, start_phase: int = 1):
    """Main crawler function with phase selection"""
    # Load config
    config = CrawlerConfig(config_name)
    base_url = base_url or config.website_config["base_url"]
    batch_size = config.processing_config["batch_size"]
    
    logger.info(f"Starting crawler from Phase {start_phase}")
    
    # Initialize variables
    failed_industries: List[tuple] = []
    industry_link_counts: Dict[str, int] = {}
    detail_tasks = []
    total_links_processed = 0
    
    # Execute phases based on start_phase
    if start_phase <= 1:
        phase1_result = await run_phase1_links(config, base_url, batch_size)
        failed_industries = phase1_result['failed_industries']
        industry_link_counts = phase1_result['industry_link_counts']
        detail_tasks = phase1_result['detail_tasks']
        total_links_processed = phase1_result['total_links_processed']
    
    if start_phase <= 2:
        await run_phase2_details(detail_tasks)
    
    if start_phase <= 3:
        await run_phase3_extract_details(batch_size)
    
    if start_phase <= 4:
        await run_phase4_contacts(batch_size)
    
    if start_phase <= 5:
        await run_phase5_extract_emails(batch_size)
    
    if start_phase <= 6:
        await run_phase6_export()
    
    # Final summary
    logger.info("=" * 80)
    logger.info("CRAWLING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total industries processed: {len(industry_link_counts)}")
    logger.info(f"Total links processed: {total_links_processed}")
    logger.info(f"Failed industries: {len(failed_industries)}")
    logger.info(f"Detail tasks submitted: {len(detail_tasks)}")
    logger.info("=" * 80)
    
    if failed_industries:
        logger.warning(f"Failed industries: {[name for _, name in failed_industries]}")
    
    logger.info("All phases completed successfully!")
    return {
        "status": "success", 
        "message": "Crawling completed successfully",
        "total_industries": len(industry_link_counts),
        "total_links": total_links_processed,
        "failed_industries": len(failed_industries),
        "detail_tasks": len(detail_tasks)
    }

def detect_completed_phases():
    """Detect which phases have been completed based on existing data"""
    completed_phases = {
        'phase1_links': False,
        'phase2_details': False,
        'phase3_contacts': False,
        'phase4_extraction': False,
        'phase5_export': False
    }
    
    # Check Phase 1: Links (checkpoint files exist)
    import os
    import glob
    checkpoint_files = glob.glob("data/checkpoint_*.json")
    if checkpoint_files:
        completed_phases['phase1_links'] = True
        logger.info(f"Phase 1 (Links) completed: {len(checkpoint_files)} checkpoint files found")
    
    # Check Phase 2: Detail HTML (database has detail_html_storage records)
    try:
        from app.database.db_manager import DatabaseManager
        db_manager = DatabaseManager()
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM detail_html_storage")
            detail_count = cursor.fetchone()[0]
            if detail_count > 0:
                completed_phases['phase2_details'] = True
                logger.info(f"Phase 2 (Detail HTML) completed: {detail_count} records found")
    except Exception as e:
        logger.warning(f"Could not check Phase 2 status: {e}")
    
    # Check Phase 3: Contact HTML (database has contact_html_storage records)
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
            contact_count = cursor.fetchone()[0]
            if contact_count > 0:
                completed_phases['phase3_contacts'] = True
                logger.info(f"Phase 3 (Contact HTML) completed: {contact_count} records found")
    except Exception as e:
        logger.warning(f"Could not check Phase 3 status: {e}")
    
    # Check Phase 4: Company Details (database has company_details records)
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM company_details")
            details_count = cursor.fetchone()[0]
            if details_count > 0:
                completed_phases['phase4_extraction'] = True
                logger.info(f"Phase 4 (Company Details) completed: {details_count} records found")
    except Exception as e:
        logger.warning(f"Could not check Phase 4 status: {e}")
    
    # Check Phase 5: Export (CSV file exists and has data)
    if os.path.exists("data/company_contacts.csv"):
        try:
            import pandas as pd
            df = pd.read_csv("data/company_contacts.csv")
            if len(df) > 0:
                completed_phases['phase5_export'] = True
                logger.info(f"Phase 5 (Export) completed: CSV file found with {len(df)} records")
            else:
                logger.info("Phase 5 (Export): CSV file exists but is empty")
        except Exception as e:
            logger.info(f"Phase 5 (Export): CSV file exists but could not read: {e}")
    else:
        logger.info("Phase 5 (Export): CSV file not found")
    
    return completed_phases

def main():
    parser = argparse.ArgumentParser(description="PCrawler - Professional Web Crawler with Phase Selection")
    parser.add_argument("command", choices=["crawl", "list-configs", "validate", "show-config"], help="Command to execute")
    parser.add_argument("--config", type=str, default="1900comvn", help="Config name (default: 1900comvn)")
    parser.add_argument("--phase", type=str, choices=["1", "2", "3", "4", "5", "auto"], default="auto", 
                       help="Start from specific phase (1=links, 2=details, 3=contacts, 4=extraction, 5=export, auto=detect)")
    parser.add_argument("--force-restart", action="store_true", help="Force restart from Phase 1 even if phases are completed")
    args = parser.parse_args()
    
    if args.command == "crawl":
        # Detect completed phases
        completed_phases = detect_completed_phases()
        
        # Determine starting phase
        if args.force_restart:
            start_phase = 1
            logger.info("Force restart: Starting from Phase 1")
        elif args.phase == "auto":
            # Auto-detect starting phase
            if not completed_phases['phase1_links']:
                start_phase = 1
            elif not completed_phases['phase2_details']:
                start_phase = 2
            elif not completed_phases['phase3_contacts']:
                start_phase = 3
            elif not completed_phases['phase4_extraction']:
                start_phase = 4
            elif not completed_phases['phase5_export']:
                start_phase = 5
            else:
                logger.info("All phases completed! Nothing to do.")
                return
            logger.info(f"Auto-detected: Starting from Phase {start_phase}")
        else:
            start_phase = int(args.phase)
            logger.info(f"Manual selection: Starting from Phase {start_phase}")
        
        # Run with phase selection
        asyncio.run(run(args.config, start_phase=start_phase))

if __name__ == "__main__":
    main()
