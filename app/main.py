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
        
        for idx, (task, ind_id, ind_name) in enumerate(link_tasks, start=1):
            try:
                result = task.get(timeout=600)  # 10 minutes timeout per industry
                logger.info(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> Task result: {result}")
                
                # Check if task was successful
                if not result or not result.get('checkpoint_file'):
                    logger.error(f"[wave {wave_index} - {idx}/{len(link_tasks)}] Industry '{ind_name}' -> FAILED; will retry later")
                    failed_industries.append((ind_id, ind_name))
                    continue
                
                # Load links from checkpoint file
                checkpoint_file = result.get('checkpoint_file')
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

    logger.info(f"Total links processed: {total_links_processed} companies across {len(industries)} industries")
    
    # Retry failed industries with longer timeout
    if failed_industries:
        logger.info(f"Retrying {len(failed_industries)} failed industries with extended timeout...")
        for ind_id, ind_name in failed_industries:
            try:
                logger.info(f"Retrying industry '{ind_name}' with 60-minute timeout...")
                retry_task = task_fetch_industry_links.delay(base_url, ind_id, ind_name, 2)
                result = retry_task.get(timeout=3600)  # 60 minutes timeout
                
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
                    logger.error(f"Retry failed for industry '{ind_name}'")
                    
            except Exception as e:
                logger.error(f"Retry failed for industry '{ind_name}': {e}")
    
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
    
    # PHASE 3: Extract company details from DB
    logger.info("PHASE 3: Extracting company details from stored HTML...")
    try:
        r = task_extract_company_details.delay(batch_size)
        r.get(timeout=3600)
    except Exception as e:
        logger.warning(f"Company details extraction encountered issues: {e}")

    # PHASE 4: Crawl contact pages (website/facebook, deep)
    logger.info("PHASE 4: Crawling contact pages (website/facebook) from company_details...")
    try:
        r = task_crawl_contact_from_details.delay(batch_size)
        r.get(timeout=3600)
    except Exception as e:
        logger.warning(f"Contact crawling encountered issues: {e}")

    # PHASE 5: Extract emails from contact HTML via Crawl4AI
    logger.info("PHASE 5: Extracting emails from contact HTML via Crawl4AI...")
    try:
        r = task_extract_emails_from_contact.delay(batch_size)
        r.get(timeout=3600)
    except Exception as e:
        logger.warning(f"Email extraction encountered issues: {e}")

    # PHASE 6: Export final CSV (join via DataFrame)
    logger.info("PHASE 6: Exporting final CSV (joining phases 1-2-4)...")
    try:
        r = task_export_final_csv.delay()
        res = r.get(timeout=1800)
        logger.info(f"Final export completed: {res.get('rows', 0)} rows -> {res.get('output')}")
    except Exception as e:
        logger.error(f"Failed exporting final CSV: {e}")
        return {"status": "error", "message": str(e)}

    # Cleanup ListCrawler resources
    try:
        await list_c.cleanup()
    except Exception as e:
        logger.warning(f"Error during ListCrawler cleanup: {e}")

    logger.info("All phases completed successfully!")
    return {"status": "success", "message": "Crawling completed successfully"}

def main():
    parser = argparse.ArgumentParser(description="PCrawler - Professional Web Crawler")
    parser.add_argument("command", choices=["crawl", "list-configs", "validate", "show-config"], help="Command to execute")
    parser.add_argument("--config", type=str, default="1900comvn", help="Config name (default: 1900comvn)")
    args = parser.parse_args()
    
    if args.command == "crawl":
        asyncio.run(run(args.config))

if __name__ == "__main__":
    main()
