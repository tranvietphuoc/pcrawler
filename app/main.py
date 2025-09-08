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
    create_final_results as task_create_final_results,
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
    for idx, (ind_id, ind_name) in enumerate(industries, start=1):
        links = await list_c.get_company_links_for_industry(base_url, ind_id, ind_name)
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

    # PHASE 5: Build final_results
    logger.info("Creating final_results table...")
    try:
        r = task_create_final_results.delay()
        res = r.get(timeout=600)
        logger.info(f"Final results created: {res.get('count', 0)} rows")
    except Exception as e:
        logger.error(f"Failed creating final results: {e}")
        return {"status": "error", "message": str(e)}

    return {
        "status": "success",
        "total_industries": len(industries),
        "total_companies": total_companies,
        "final_table": "final_results",
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


async def backup_crawl_direct(file_path: str, batch_size: int):
    """Backup crawl trực tiếp không dùng Celery"""
    try:
        config = CrawlerConfig()
        backup_crawler = BackupCrawler(config)
        
        # Load N/A rows
        na_rows = backup_crawler.load_merged_file(file_path)
        
        if na_rows.empty:
            logger.info("No N/A emails found")
            return
        
        logger.info(f"Found {len(na_rows)} N/A rows, starting deep crawl...")
        
        # Process theo batch
        backup_results = []
        total_rows = len(na_rows)
        
        for i in range(0, total_rows, batch_size):
            batch = na_rows.iloc[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size}")
            
            # Process batch
            batch_results = []
            for _, row in batch.iterrows():
                try:
                    result = await backup_crawler.deep_crawl_emails(row)
                    batch_results.append(result)
                    
                    if result['extracted_emails'] != 'N/A':
                        logger.info(f"✓ Found emails for {row['name']}: {result['extracted_emails']}")
                    else:
                        logger.warning(f"✗ No emails found for {row['name']}")
                        
                except Exception as e:
                    logger.error(f"Error processing {row['name']}: {e}")
                    continue
            
            backup_results.extend(batch_results)
        
        # Update file (ghi đè file gốc)
        updated_file = backup_crawler.update_merged_file(file_path, backup_results)
        
        # Summary
        updated_count = len([r for r in backup_results if r['extracted_emails'] != 'N/A'])
        logger.info(f"Deep crawl completed: {updated_count}/{total_rows} N/A rows updated")
        logger.info(f"Updated file: {updated_file}")
        
    except Exception as e:
        logger.error(f"Deep crawl failed: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="PCrawler - Professional Web Crawler")
    
    # Main commands
    parser.add_argument(
        "command",
        choices=["crawl", "backup-check", "backup-crawl", "backup-deep"],
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
        
    elif args.command == "backup-check":
        # Check và backup crawl (dùng task)
        logger.info("Checking for merged file and starting backup crawl...")
        result = check_and_backup_crawl(args.file)
        print(f"Result: {result}")
        
    elif args.command == "backup-crawl":
        # Backup crawl với Celery task
        if not args.file:
            logger.error("--file is required for backup-crawl command")
            sys.exit(1)
            
        logger.info(f"Starting backup crawl for file: {args.file}")
        task = backup_crawl_na_emails.delay(args.file, args.batch_size)
        print(f"Backup crawl task started: {task.id}")
        print(f"Monitor progress with: celery -A app.tasks.celery_app inspect active")
        
    elif args.command == "backup-deep":
        # Deep crawl trực tiếp (không dùng Celery)
        if not args.file:
            logger.error("--file is required for backup-deep command")
            sys.exit(1)
            
        logger.info(f"Starting deep crawl for file: {args.file}")
        asyncio.run(backup_crawl_direct(args.file, args.batch_size))


if __name__ == "__main__":
    main()
