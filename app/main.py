import asyncio, argparse, logging
import os
import uuid
import pandas as pd
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
import sys


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("main")


async def run(
    config_name: str = "default",
    base_url: str = None,
    output_dir: str = None,
    final_output_path: str = None,
    write_batch_size: int = None,
    max_concurrent_pages: int = None,
    log_level: str = "INFO",
    log_file: str = None,
):
    # Load config
    config = CrawlerConfig(config_name)
    
    # Use config values if not provided
    base_url = base_url or config.website_config["base_url"]
    output_dir = output_dir or config.output_config["output_dir"]
    final_output_path = final_output_path or config.output_config["final_output"]
    write_batch_size = write_batch_size or config.processing_config["write_batch_size"]
    max_concurrent_pages = max_concurrent_pages or config.processing_config["max_concurrent_pages"]
    batch_size = config.processing_config["batch_size"]
    
    # Tạo thư mục output nếu chưa có
    os.makedirs(output_dir, exist_ok=True)
    
    # BỎ luồng backup-merger cũ. Luồng mới hoàn toàn chạy theo DB phases
    
    # NẾU CHƯA CÓ FILE MERGED → CRAWL TỪ ĐẦU
    logger.info("No merged file found, starting full crawl from beginning...")
    
    # Crawl industries - giữ nguyên logic cũ
    list_c = ListCrawler(config=config)
    industries = await list_c.get_industries(base_url)
    logger.info(f"Found {len(industries)} industries")
    
    # PHASE 1: Crawl detail pages -> DB
    detail_tasks = []
    total_companies = 0
    all_company_links: List[Dict[str, Any]] = []
    
    # Load existing checkpoints
    import json
    import re
    checkpoint_dir = "/tmp"
    if os.path.exists(checkpoint_dir):
        for filename in os.listdir(checkpoint_dir):
            if filename.startswith("checkpoint_") and filename.endswith(".json"):
                try:
                    with open(os.path.join(checkpoint_dir, filename), 'r') as f:
                        checkpoint_data = json.load(f)
                        if checkpoint_data:
                            # Extract industry name from sanitized filename
                            # Format: checkpoint_{sanitized_name}_{pass_no}.json
                            parts = filename.replace("checkpoint_", "").replace(".json", "").split("_")
                            if len(parts) >= 2:
                                # Reconstruct original industry name (approximate)
                                sanitized_name = "_".join(parts[:-1])  # All parts except last (pass_no)
                                # This is approximate - we can't perfectly reconstruct the original name
                                industry_name = sanitized_name.replace("_", " ")
                            else:
                                industry_name = filename
                            
                            all_company_links.extend(checkpoint_data)
                            logger.info(f"Loaded checkpoint: {industry_name} -> {len(checkpoint_data)} links")
                except Exception as e:
                    logger.warning(f"Failed to load checkpoint {filename}: {e}")
    
    if all_company_links:
        logger.info(f"Loaded {len(all_company_links)} links from checkpoints")
    async def fetch_links_with_retry(ind_id: str, ind_name: str, pass_no: int = 1) -> List[str]:
        # Adaptive retries/timeouts per pass (tăng để giảm miss ở industry lớn)
        if pass_no == 1:
            retries, timeout_s, delay_s = 5, 600, 5  # Tăng timeout lên 10 phút, 5 retries
        else:
            retries, timeout_s, delay_s = 6, 900, 10  # Tăng timeout lên 15 phút cho pass 2+
        links_local: List[str] = []
        logger.info(f"[{ind_name}] Start fetching (pass {pass_no}) with timeout={timeout_s}s, retries={retries}")
        for attempt in range(retries + 1):
            try:
                # Progressive timeout: tăng timeout mỗi attempt
                current_timeout = timeout_s + (attempt * 60)  # +60s mỗi attempt
                logger.info(f"[{ind_name}] Attempt {attempt+1}/{retries+1} (pass {pass_no}) with timeout={current_timeout}s")
                links_local = await asyncio.wait_for(
                    list_c.get_company_links_for_industry(base_url, ind_id, ind_name),
                    timeout=current_timeout,
                )
                logger.info(f"[{ind_name}] Success (pass {pass_no}) -> {len(links_local)} links")
                if links_local:
                    return links_local
            except asyncio.TimeoutError:
                logger.warning(f"[{ind_name}] Timeout on attempt {attempt+1}/{retries+1} (pass {pass_no})")
            except Exception as e:
                logger.warning(f"[{ind_name}] Error on attempt {attempt+1}/{retries+1} (pass {pass_no}): {e}")
                
                # Handle TargetClosedError specifically
                if "Target page, context or browser has been closed" in str(e) or "TargetClosedError" in str(e):
                    logger.warning(f"[{ind_name}] TargetClosedError detected, forcing browser restart...")
                    try:
                        await list_c.cleanup()
                        await asyncio.sleep(5)  # Wait longer for cleanup
                        # Browser will be created automatically by context manager
                    except Exception as cleanup_error:
                        logger.warning(f"[{ind_name}] Browser restart failed: {cleanup_error}")
                
                # If browser context error, try to restart browser
                elif "Browser.new_context" in str(e) or "browser has been closed" in str(e):
                    logger.warning(f"[{ind_name}] Browser context error detected, attempting browser restart...")
                    try:
                        await list_c.cleanup()
                        await asyncio.sleep(3)  # Wait longer for cleanup
                    except Exception as cleanup_error:
                        logger.warning(f"[{ind_name}] Browser cleanup failed: {cleanup_error}")
                
                # Random uniform delay
                import random
                min_delay = delay_s * 0.5
                max_delay = delay_s * 1.5
                random_delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(random_delay)
        logger.error(f"[{ind_name}] Failed after {retries+1} attempts (pass {pass_no})")
        return links_local

    failed_industries: List[tuple] = []
    industry_link_counts: Dict[str, int] = {}

    # PHASE 0 - PASS 1: Fetch all company links for all industries (PARALLEL)
    logger.info("PHASE 0 - PASS 1: Fetching all company links in parallel...")
    
    # Submit all industry link fetching tasks in parallel
    link_tasks = []
    for idx, (ind_id, ind_name) in enumerate(industries, start=1):
        logger.info(f"[{idx}/{len(industries)}] Submitting link fetching task for industry '{ind_name}'")
        task = task_fetch_industry_links.delay(base_url, ind_id, ind_name, 1)
        link_tasks.append((task, ind_id, ind_name))
    
    # Collect results from parallel tasks
    logger.info(f"Waiting for {len(link_tasks)} industry link fetching tasks to complete...")
    for idx, (task, ind_id, ind_name) in enumerate(link_tasks, start=1):
        try:
            result = task.get(timeout=1200)  # 20 minutes timeout per industry
            if not result or result.get('status') != 'success':
                logger.error(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> FAILED on pass 1; will retry later")
                failed_industries.append((ind_id, ind_name))
                continue
            
            # Get links from checkpoint file
            checkpoint_file = result.get('checkpoint_file')
            if not checkpoint_file:
                logger.error(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> No checkpoint file; will retry later")
                failed_industries.append((ind_id, ind_name))
                continue
            
            # Load links from checkpoint
            try:
                import json
                import os
                # Tạo thư mục data nếu chưa tồn tại
                os.makedirs('/app/data', exist_ok=True)
                with open(checkpoint_file, 'r') as f:
                    links = json.load(f)
                logger.info(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> Loaded {len(links)} links from checkpoint")
            except Exception as e:
                logger.error(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> Failed to load checkpoint: {e}; will retry later")
                failed_industries.append((ind_id, ind_name))
                continue
            
            # Check if we got reasonable number of links (industry size validation)
            if len(links) < 10:  # Industry quá nhỏ, có thể bị miss
                logger.warning(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> Only {len(links)} links (suspiciously low)")
                # Try one more time with longer timeout
                logger.info(f"[{idx}/{len(industries)}] Retrying '{ind_name}' with extended timeout...")
                retry_task = task_fetch_industry_links.delay(base_url, ind_id, ind_name, 1)
                retry_result = retry_task.get(timeout=1200)
                if retry_result and retry_result.get('status') == 'success':
                    retry_checkpoint = retry_result.get('checkpoint_file')
                    if retry_checkpoint:
                        try:
                            with open(retry_checkpoint, 'r') as f:
                                links_retry = json.load(f)
                            if len(links_retry) > len(links):
                                links = links_retry
                                logger.info(f"[{idx}/{len(industries)}] Retry successful: {len(links)} links")
                        except Exception as e:
                            logger.warning(f"[{idx}/{len(industries)}] Failed to load retry checkpoint: {e}")
            
            logger.info(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> {len(links)} companies")
            all_company_links.extend(links)
            industry_link_counts[ind_name] = len(links)
            
        except Exception as e:
            logger.error(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> FAILED: {e}")
            failed_industries.append((ind_id, ind_name))
    
    # Submit detail crawling tasks for all collected links
    logger.info(f"Submitting detail crawling tasks for {len(all_company_links)} companies...")
    for i in range(0, len(all_company_links), batch_size):
        batch = all_company_links[i:i+batch_size]
        task = task_crawl_detail_pages.delay(batch, batch_size)
        detail_tasks.append(task)

    # PHASE 0 - PASS 2: Retry failed industries
    if failed_industries:
        logger.info(f"PHASE 0 - PASS 2: Retrying {len(failed_industries)} industries...")
        for ind_id, ind_name in failed_industries:
            # Create fresh browser for each retry industry
            logger.info(f"[Retry] Creating fresh browser for industry '{ind_name}'")
            # Browser will be created automatically by context manager
            
            links = await fetch_links_with_retry(ind_id, ind_name, pass_no=2)
            if not links:
                logger.error(f"[Retry] Industry '{ind_name}' -> still FAILED to fetch links, skipping")
                continue
            logger.info(f"[Retry] Industry '{ind_name}' -> {len(links)} companies")
            normalized: List[Dict[str, Any]] = []
            for item in links:
                if isinstance(item, str):
                    normalized.append({
                        'name': '',
                        'url': item,
                        'industry': ind_name,
                    })
                elif isinstance(item, dict):
                    item = {**item}
                    item['industry'] = ind_name
                    normalized.append(item)
            all_company_links.extend(normalized)
            industry_link_counts[ind_name] = industry_link_counts.get(ind_name, 0) + len(normalized)
            
            # Submit detail batches ngay cho pass 2
            for i in range(0, len(normalized), batch_size):
                batch = normalized[i:i+batch_size]
                t = task_crawl_detail_pages.delay(batch, batch_size)
                detail_tasks.append(t)
            
            # Random uniform delay giữa các industry retry
            import random
            await asyncio.sleep(random.uniform(1, 3))

    # Báo cáo industry nào còn 0 link sau 2 pass (để đảm bảo không sót)
    zero_link_industries = [name for name in [n for _, n in industries] if industry_link_counts.get(name, 0) == 0]
    if zero_link_industries:
        logger.warning(f"Industries with 0 links after 2 passes: {len(zero_link_industries)} -> {zero_link_industries}")
        
        # PHASE 0 - PASS 3: Final attempt for zero-link industries with aggressive settings
        logger.info(f"PHASE 0 - PASS 3: Final attempt for {len(zero_link_industries)} zero-link industries...")
        for ind_id, ind_name in [(id, name) for id, name in industries if name in zero_link_industries]:
            logger.info(f"[Final Attempt] Trying industry '{ind_name}' with aggressive settings...")
            
            # Create fresh browser for final attempt
            logger.info(f"[Final Attempt] Creating fresh browser for industry '{ind_name}'")
            # Browser will be created automatically by context manager
            
            # Aggressive retry settings for final attempt
            retries, timeout_s, delay_s = 5, 1200, 10  # 20 minutes timeout, 5 retries, 10s delay
            links_local = []
            
            for attempt in range(retries + 1):
                try:
                    logger.info(f"[Final Attempt] {ind_name} - Attempt {attempt+1}/{retries+1} (timeout={timeout_s}s)")
                    links_local = await asyncio.wait_for(
                        list_c.get_company_links_for_industry(base_url, ind_id, ind_name),
                        timeout=timeout_s,
                    )
                    logger.info(f"[Final Attempt] {ind_name} - Success -> {len(links_local)} links")
                    if links_local:
                        break
                except asyncio.TimeoutError:
                    logger.warning(f"[Final Attempt] {ind_name} - Timeout on attempt {attempt+1}/{retries+1}")
                except Exception as e:
                    logger.warning(f"[Final Attempt] {ind_name} - Error on attempt {attempt+1}/{retries+1}: {e}")
                # Random uniform delay for final attempt
                import random
                min_delay = delay_s * 0.5
                max_delay = delay_s * 1.5
                random_delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(random_delay)
            
            if links_local:
                logger.info(f"[Final Attempt] {ind_name} - SUCCESS! Got {len(links_local)} links")
                # Process and submit links
                normalized = []
                for item in links_local:
                    if isinstance(item, str):
                        normalized.append({
                            'name': '',
                            'url': item,
                            'industry': ind_name,
                        })
                    elif isinstance(item, dict):
                        item = {**item}
                        item['industry'] = ind_name
                        normalized.append(item)
                
                all_company_links.extend(normalized)
                industry_link_counts[ind_name] = len(normalized)
                
                # Submit detail batches với delay để tránh overload
                for i in range(0, len(normalized), batch_size):
                    batch = normalized[i:i+batch_size]
                    t = task_crawl_detail_pages.delay(batch, batch_size)
                    detail_tasks.append(t)
                    # Random uniform delay nhỏ giữa các batch để tránh overload
                    if i % (batch_size * 5) == 0:  # Delay mỗi 5 batches
                        import random
                        await asyncio.sleep(random.uniform(1, 3))
                
                # Random uniform delay for final attempt
                import random
                await asyncio.sleep(random.uniform(3, 7))
            else:
                logger.error(f"[Final Attempt] {ind_name} - FAILED after {retries+1} attempts, giving up")
        
        # Final report
        final_zero_industries = [name for name in [n for _, n in industries] if industry_link_counts.get(name, 0) == 0]
        if final_zero_industries:
            logger.error(f"FINAL RESULT: {len(final_zero_industries)} industries still have 0 links: {final_zero_industries}")
        else:
            logger.info("FINAL RESULT: All industries now have links!")

    # PHASE 1: Tổng kết và chờ tất cả detail batches (đã submit dần trong lúc fetch)
    total_companies = len(all_company_links)
    logger.info(f"Total unique company links collected: {total_companies}")
    logger.info(f"Submitted {len(detail_tasks)} detail batches. Waiting for completion...")

    # Wait for all detail batches to complete (non-blocking with progress)
    completed = 0
    failed = 0
    for i, t in enumerate(detail_tasks):
        try:
            result = t.get(timeout=7200)  # Tăng timeout lên 2 giờ
            completed += 1
            if i % 10 == 0:  # Log progress every 10 batches
                logger.info(f"Detail batches progress: {completed}/{len(detail_tasks)} completed, {failed} failed")
        except Exception as e:
            failed += 1
            logger.warning(f"Detail batch {i+1} failed: {e}")
    
    logger.info(f"PHASE 1 COMPLETED: {completed} successful, {failed} failed out of {len(detail_tasks)} batches")

    # PHASE 2: Extract company details from DB
    logger.info("Extracting company details from stored HTML...")
    try:
        r = task_extract_company_details.delay(batch_size)
        r.get(timeout=3600)
    except Exception as e:
        logger.warning(f"Company details extraction encountered issues: {e}")

    # PHASE 3: Crawl contact pages (website/facebook, deep)
    logger.info("Crawling contact pages (website/facebook) from company_details...")
    try:
        r = task_crawl_contact_from_details.delay(batch_size)
        r.get(timeout=3600)
    except Exception as e:
        logger.warning(f"Contact crawling encountered issues: {e}")

    # PHASE 4: Extract emails from contact HTML via Crawl4AI
    logger.info("Extracting emails from contact HTML via Crawl4AI...")
    try:
        r = task_extract_emails_from_contact.delay(batch_size)
        r.get(timeout=3600)
    except Exception as e:
        logger.warning(f"Email extraction encountered issues: {e}")

    # PHASE 5: Export final CSV (join via DataFrame)
    logger.info("Exporting final CSV (joining phases 1-2-4)...")
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
        logger.warning(f"Error cleaning up ListCrawler: {e}")

        return {
            "status": "success",
            "total_industries": len(industries),
        "total_companies": total_companies,
        "final_output": res.get('output'),
        }




# Legacy backup removed


def main():
    parser = argparse.ArgumentParser(description="PCrawler - Professional Web Crawler")
    
    # Main commands
    parser.add_argument(
        "command",
        choices=["crawl", "list-configs", "validate", "show-config"],
        help="Command to execute"
    )
    
    # Config
    parser.add_argument(
        "--config",
        type=str,
        default="1900comvn",
        help="Config name (default: 1900comvn)"
    )
    
    # Backup options
    parser.add_argument(
        "--file",
        type=str,
        help="Path to merged CSV file for backup commands"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for backup processing (default: 10)"
    )
    
    args = parser.parse_args()
    
    if args.command == "crawl":
        # Normal crawl (đã có logic check file merged trong run())
        asyncio.run(run(args.config))
        
    # Legacy backup commands removed


if __name__ == "__main__":
    main()
