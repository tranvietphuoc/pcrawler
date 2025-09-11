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

async def run(config_name: str = "default", base_url: str = None):
    # Load config
    config = CrawlerConfig(config_name)
    base_url = base_url or config.website_config["base_url"]
    batch_size = config.processing_config["batch_size"]
    
    # Get industries
    list_c = ListCrawler(config=config)
    industries = await list_c.get_industries(base_url)
    logger.info(f"Found {len(industries)} industries")
    
    # PHASE 1: Crawl links for all industries and save checkpoints
    logger.info("PHASE 1: Crawling links for all industries...")
    
    failed_industries: List[tuple] = []
    industry_link_counts: Dict[str, int] = {}
    detail_tasks = []
    
    # Submit link fetching tasks in small waves to avoid overload
    wave_size = config.processing_config.get("industry_wave_size", 4)
    total_links_processed = 0

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
        logger.info(f"Waiting for {len(link_tasks)} industry link fetching tasks in wave {wave_index} to complete...")
        
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
                    
                    # Submit detail crawling tasks in batches
                    logger.info(f"Submitting detail crawling tasks for industry '{ind_name}' ({total_links} companies) in batches...")
                    batch_count = 0
                    for i in range(0, len(links), batch_size):
                        batch = links[i:i+batch_size]
                        batch_count += 1
                        task = task_crawl_detail_pages.delay(batch, batch_size)
                        detail_tasks.append(task)
                        if batch_count % 10 == 0:  # Log progress every 10 batches
                            logger.info(f"[wave {wave_index}] Submitted {batch_count} batches for industry '{ind_name}'...")
                    
                    total_links_processed += total_links
                    industry_link_counts[ind_name] = total_links
                    
                    # Clear links from memory
                    del links
                    
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
                    
                    # Submit detail tasks
                    for i in range(0, len(links), batch_size):
                        batch = links[i:i+batch_size]
                        task = task_crawl_detail_pages.delay(batch, batch_size)
                        detail_tasks.append(task)
                    
                    total_links_processed += total_links
                    industry_link_counts[ind_name] = total_links
                    del links
                else:
                    error_msg = result.get('error', 'No checkpoint file') if result else 'No result returned'
                    logger.error(f"Retry failed for industry '{ind_name}': {error_msg}")
                    
            except Exception as e:
                logger.error(f"Retry failed for industry '{ind_name}': {e}")
        
        logger.info(f"Retry phase completed: {completed_retries}/{len(retry_tasks)} tasks processed")
    
    # PHASE 2: Wait for all detail crawling tasks to complete
    if detail_tasks:
        logger.info(f"PHASE 2: Waiting for {len(detail_tasks)} detail crawling tasks to complete...")
        completed = 0
        failed = 0
        for i, t in enumerate(detail_tasks):
            try:
                result = t.get(timeout=1800)  # 30 minutes timeout per batch
                completed += 1
                if i % 10 == 0:  # Log progress every 10 batches
                    logger.info(f"Detail batches progress: {completed}/{len(detail_tasks)} completed, {failed} failed")
            except Exception as e:
                failed += 1
                logger.warning(f"Detail batch {i+1} failed: {e}")
        
        logger.info(f"DETAIL CRAWLING COMPLETED: {completed} successful, {failed} failed out of {len(detail_tasks)} batches")
    else:
        logger.warning("No detail crawling tasks to process - all industries failed in Phase 1")
    
    # PHASE 3: Extract company details from DB
    logger.info("PHASE 3: Extracting company details from stored HTML...")
    try:
        r = task_extract_company_details.delay(batch_size)
        result = r.get(timeout=3600)
        if result:
            logger.info(f"Company details extraction completed: {result.get('processed', 0)} companies processed")
        else:
            logger.warning("Company details extraction returned None result")
    except Exception as e:
        logger.warning(f"Company details extraction encountered issues: {e}")

    # PHASE 4: Crawl contact pages (website/facebook, deep)
    logger.info("PHASE 4: Crawling contact pages (website/facebook) from company_details...")
    try:
        r = task_crawl_contact_from_details.delay(batch_size)
        result = r.get(timeout=3600)
        if result:
            logger.info(f"Contact crawling completed: {result.get('processed', 0)} companies processed")
        else:
            logger.warning("Contact crawling returned None result")
    except Exception as e:
        logger.warning(f"Contact crawling encountered issues: {e}")

    # PHASE 5: Extract emails from contact HTML via Crawl4AI
    logger.info("PHASE 5: Extracting emails from contact HTML via Crawl4AI...")
    try:
        r = task_extract_emails_from_contact.delay(batch_size)
        result = r.get(timeout=3600)
        if result:
            logger.info(f"Email extraction completed: {result.get('processed', 0)} companies processed")
        else:
            logger.warning("Email extraction returned None result")
    except Exception as e:
        logger.warning(f"Email extraction encountered issues: {e}")

    # PHASE 6: Export final CSV (join via DataFrame)
    logger.info("PHASE 6: Exporting final CSV (joining phases 1-2-4)...")
    try:
        r = task_export_final_csv.delay()
        res = r.get(timeout=1800)
        if res:
            logger.info(f"Final export completed: {res.get('rows', 0)} rows -> {res.get('output')}")
        else:
            logger.warning("Final export returned None result")
    except Exception as e:
        logger.error(f"Failed exporting final CSV: {e}")
        return {"status": "error", "message": str(e)}

    # Cleanup ListCrawler resources
    try:
        await list_c.cleanup()
    except Exception as e:
        logger.warning(f"Error during ListCrawler cleanup: {e}")

    # Final summary
    logger.info("=" * 80)
    logger.info("CRAWLING SUMMARY:")
    logger.info(f"Total industries processed: {len(industries)}")
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
        "total_industries": len(industries),
        "total_links": total_links_processed,
        "failed_industries": len(failed_industries),
        "detail_tasks": len(detail_tasks)
    }

def main():
    parser = argparse.ArgumentParser(description="PCrawler - Professional Web Crawler")
    parser.add_argument("command", choices=["crawl", "list-configs", "validate", "show-config"], help="Command to execute")
    parser.add_argument("--config", type=str, default="1900comvn", help="Config name (default: 1900comvn)")
    args = parser.parse_args()
    
    if args.command == "crawl":
        asyncio.run(run(args.config))

if __name__ == "__main__":
    main()
