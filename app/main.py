import asyncio, argparse, logging
import os
import uuid
import pandas as pd
from typing import List, Dict, Any
from app.crawler.list_crawler import ListCrawler
from app.tasks.html_tasks import (
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
    async def fetch_links_with_retry(ind_id: str, ind_name: str, pass_no: int = 1) -> List[str]:
        # Adaptive retries/timeouts per pass
        if pass_no == 1:
            retries, timeout_s, delay_s = 2, 120, 2
        else:
            retries, timeout_s, delay_s = 3, 180, 3
        links_local: List[str] = []
        for attempt in range(retries + 1):
            try:
                links_local = await asyncio.wait_for(
                    list_c.get_company_links_for_industry(base_url, ind_id, ind_name),
                    timeout=timeout_s,
                )
                if links_local:
                    return links_local
            except asyncio.TimeoutError:
                logger.warning(f"[Industry {ind_name}] timeout (attempt {attempt+1}/{retries+1}, pass {pass_no})")
            except Exception as e:
                logger.warning(f"[Industry {ind_name}] error (attempt {attempt+1}/{retries+1}, pass {pass_no}): {e}")
            await asyncio.sleep(delay_s)
        return links_local

    failed_industries: List[tuple] = []

    for idx, (ind_id, ind_name) in enumerate(industries, start=1):
        links = await fetch_links_with_retry(ind_id, ind_name, pass_no=1)
        if not links:
            logger.error(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> FAILED on pass 1; will retry later")
            failed_industries.append((ind_id, ind_name))
            continue
        logger.info(f"[{idx}/{len(industries)}] Industry '{ind_name}' -> {len(links)} companies")
        total_companies += len(links)
        # Chuẩn hoá dữ liệu company
        normalized = []
        for item in links:
            if isinstance(item, str):
                normalized.append({
                    'name': '',
                    'url': item,
                })
            elif isinstance(item, dict):
                item = {**item}
                normalized.append(item)
        for i in range(0, len(normalized), batch_size):
            batch = normalized[i:i+batch_size]
            t = task_crawl_detail_pages.delay(batch, batch_size)
            detail_tasks.append(t)
    logger.info(f"Submitted {len(detail_tasks)} detail batches")

    # Wait detail tasks
    for t in detail_tasks:
        try:
            t.get(timeout=3600)
        except Exception as e:
            logger.warning(f"Detail batch failed: {e}")

    # Second pass for industries that failed to fetch links
    if failed_industries:
        logger.info(f"Retrying {len(failed_industries)} industries in second pass...")
        for ind_id, ind_name in failed_industries:
            links = await fetch_links_with_retry(ind_id, ind_name, pass_no=2)
            if not links:
                logger.error(f"[Retry] Industry '{ind_name}' -> still FAILED to fetch links, skipping")
                continue
            logger.info(f"[Retry] Industry '{ind_name}' -> {len(links)} companies")
            normalized = []
            for item in links:
                if isinstance(item, str):
                    normalized.append({
                        'name': '',
                        'url': item,
                    })
                elif isinstance(item, dict):
                    item = {item: item for item in item} if isinstance(item, dict) else item
                    normalized.append(item)
            for i in range(0, len(normalized), batch_size):
                batch = normalized[i:i+batch_size]
                t = task_crawl_detail_pages.delay(batch, batch_size)
                try:
                    t.get(timeout=3600)
                except Exception as e:
                    logger.warning(f"Detail batch (retry) failed for '{ind_name}': {e}")

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

    return {
        "status": "success",
        "total_industries": len(industries),
        "total_companies": total_companies,
        "final_output": res.get('output'),
    }


def main():
    p = argparse.ArgumentParser(description="P Crawler - Modular web crawler system")
    subparsers = p.add_subparsers(dest="command", help="Available commands")
    
    # Crawl command
    crawl_parser = subparsers.add_parser("crawl", help="Start crawling process")
    crawl_parser.add_argument("--config", default="default", help="Configuration name (default, 1900comvn, example)")
    crawl_parser.add_argument("--base-url", help="Override base URL from config")
    crawl_parser.add_argument("--output-dir", help="Override output directory from config")
    crawl_parser.add_argument("--final-output", help="Override final output path from config")
    crawl_parser.add_argument("--write-batch-size", type=int, help="Override write batch size from config")
    crawl_parser.add_argument("--max-concurrent-pages", type=int, help="Override max concurrent pages from config")
    crawl_parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level")
    crawl_parser.add_argument("--log-file", help="Log file path (optional)")
    
    # List configs command
    list_parser = subparsers.add_parser("list-configs", help="List available configurations")
    
    # Validate config command
    validate_parser = subparsers.add_parser("validate", help="Validate configuration")
    validate_parser.add_argument("--config", default="default", help="Configuration name to validate")
    
    # Show config command
    show_parser = subparsers.add_parser("show-config", help="Show configuration details")
    show_parser.add_argument("--config", default="default", help="Configuration name to show")
    
    a = p.parse_args()
    
    if a.command == "list-configs":
        config = CrawlerConfig()
        configs = config.list_available_configs()
        print("Available configurations:")
        for cfg in configs:
            print(f"  - {cfg}")
        return
    
    elif a.command == "validate":
        try:
            config = CrawlerConfig(a.config)
            is_valid, errors = config.validate_config()
            if is_valid:
                print(f"Configuration '{a.config}' is valid")
            else:
                print(f"Configuration '{a.config}' has errors:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)
        except Exception as e:
            print(f"Error validating config: {e}")
            sys.exit(1)
        return
    
    elif a.command == "show-config":
        try:
            config = CrawlerConfig(a.config)
            print(f"Configuration: {a.config}")
            print(f"Website: {config.website_config.get('name', 'N/A')}")
            print(f"Base URL: {config.website_config.get('base_url', 'N/A')}")
            print(f"Batch Size: {config.processing_config.get('batch_size', 'N/A')}")
            print(f"Output Dir: {config.output_config.get('output_dir', 'N/A')}")
            print(f"Final Output: {config.output_config.get('final_output', 'N/A')}")
            print(f"Fields: {len(config.fieldnames)} fields")
        except Exception as e:
            print(f"Error showing config: {e}")
            sys.exit(1)
        return
    
    elif a.command == "crawl":
        try:
            result = asyncio.run(run(
                config_name=a.config,
                base_url=a.base_url,
                output_dir=a.output_dir,
                final_output_path=a.final_output,
                write_batch_size=a.write_batch_size,
                max_concurrent_pages=a.max_concurrent_pages,
                log_level=a.log_level,
                log_file=a.log_file,
            ))
            
            if result["status"] == "error":
                sys.exit(1)
            elif result["status"] == "warning":
                sys.exit(2)
            else:
                print(f"Crawling completed successfully!")
                print(f"Summary: {result['total_industries']} industries, {result['total_companies']} companies")
                print(f"Output: {result['final_file']} ({result['total_rows']} rows)")
                
        except KeyboardInterrupt:
            print("\nCrawling interrupted by user")
            sys.exit(130)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)
    
    else:
        # Default to crawl command for backward compatibility
        try:
            result = asyncio.run(run(
                config_name=a.config if hasattr(a, 'config') else "default",
                base_url=a.base_url if hasattr(a, 'base_url') else None,
                output_dir=a.output_dir if hasattr(a, 'output_dir') else None,
                final_output_path=a.final_output if hasattr(a, 'final_output') else None,
                write_batch_size=a.write_batch_size if hasattr(a, 'write_batch_size') else None,
                max_concurrent_pages=a.max_concurrent_pages if hasattr(a, 'max_concurrent_pages') else None,
                log_level=a.log_level if hasattr(a, 'log_level') else "INFO",
                log_file=a.log_file if hasattr(a, 'log_file') else None,
            ))
            
            if result["status"] == "error":
                sys.exit(1)
            elif result["status"] == "warning":
                sys.exit(2)
            else:
                print(f"Crawling completed successfully!")
                print(f"Summary: {result['total_industries']} industries, {result['total_companies']} companies")
                print(f"Output: {result['final_file']} ({result['total_rows']} rows)")
                
        except KeyboardInterrupt:
            print("\nCrawling interrupted by user")
            sys.exit(130)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)


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
