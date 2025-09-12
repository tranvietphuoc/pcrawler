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
    
    # Retry failed industries with proper async handling
    if failed_industries:
        logger.info(f"Retrying {len(failed_industries)} failed industries with extended timeout...")
        retry_tasks = []
        
        # Submit all retry tasks first
        for ind_id, ind_name in failed_industries:
            logger.info(f"Submitting retry task for industry '{ind_name}'...")
            retry_task = task_fetch_industry_links.delay(base_url, ind_id, ind_name, 2)
            retry_tasks.append((retry_task, ind_id, ind_name))
        
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

async def run_phase3_contacts(batch_size):
    """Phase 3: Crawl contact pages from company details"""
    logger.info("=" * 80)
    logger.info("PHASE 3: Crawling contact pages from company details...")
    logger.info("=" * 80)
    
    # Submit contact crawling task
    contact_task = task_crawl_contact_from_details.delay(batch_size)
    logger.info("Contact crawling task submitted")
    
    # Wait for completion
    try:
        result = contact_task.get(timeout=7200)  # 2 hours timeout
        logger.info(f"Contact crawling completed: {result}")
    except Exception as e:
        logger.error(f"Contact crawling failed: {e}")

async def run_phase4_extraction(batch_size):
    """Phase 4: Extract company details and emails"""
    logger.info("=" * 80)
    logger.info("PHASE 4: Extracting company details and emails...")
    logger.info("=" * 80)
    
    # Submit extraction tasks
    details_task = task_extract_company_details.delay(batch_size)
    emails_task = task_extract_emails_from_contact.delay(batch_size)
    
    logger.info("Extraction tasks submitted")
    
    # Wait for completion
    try:
        details_result = details_task.get(timeout=3600)  # 1 hour timeout
        emails_result = emails_task.get(timeout=3600)  # 1 hour timeout
        logger.info(f"Details extraction completed: {details_result}")
        logger.info(f"Emails extraction completed: {emails_result}")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")

async def run_phase5_export():
    """Phase 5: Export final CSV"""
    logger.info("=" * 80)
    logger.info("PHASE 5: Exporting final CSV...")
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
    
    logger.info(f"🚀 Starting crawler from Phase {start_phase}")
    
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
        await run_phase3_contacts(batch_size)
    
    if start_phase <= 4:
        await run_phase4_extraction(batch_size)
    
    if start_phase <= 5:
        await run_phase5_export()
    
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
    
    # Check Phase 5: Export (CSV file exists)
    if os.path.exists("data/company_contacts.csv"):
        completed_phases['phase5_export'] = True
        logger.info("Phase 5 (Export) completed: CSV file found")
    
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
