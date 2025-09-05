import asyncio, argparse, logging
import os
import uuid
from typing import List, Dict, Any
from app.crawler.list_crawler import ListCrawler
from app.crawler.detail_crawler import DetailCrawler
from app.tasks.tasks import crawl_details_extract_write, merge_csv_files
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
    
    # Crawl industries - giữ nguyên logic cũ
    list_c = ListCrawler(config=config)
    industries = await list_c.get_industries(base_url)
    logger.info(f"Found {len(industries)} industries")
    
    # Lưu danh sách task để theo dõi
    task_results = []
    
    for idx, (ind_id, ind_name) in enumerate(industries, start=1):
        links = await list_c.get_company_links_for_industry(base_url, ind_id, ind_name)
        logger.info(
            f"[{idx}/{len(industries)}] Industry '{ind_name}' -> {len(links)} companies"
        )
        
        for i in range(0, len(links), batch_size):
            batch_links = links[i : i + batch_size]
            # Tạo task_id duy nhất cho mỗi batch
            task_id = str(uuid.uuid4())
            
            result = crawl_details_extract_write.delay(
                batch_links,
                ind_name,
                output_dir,
                task_id,
                config_name,
                max_concurrent_pages,
                write_batch_size,
            )
            task_results.append(result)
            logger.info(f"Task created: {task_id} for batch {i//batch_size + 1} of industry {ind_name}")

    # Chờ tất cả task hoàn thành
    logger.info(f"Waiting for {len(task_results)} tasks to complete...")
    completed_tasks = 0
    failed_tasks = 0
    
    for result in task_results:
        try:
            task_info = result.get(timeout=3600)  # timeout 1 giờ
            logger.info(f"Task {task_info['task_id']} completed: {task_info['rows_processed']} rows")
            completed_tasks += 1
        except Exception as e:
            logger.error(f"Task failed: {e}")
            failed_tasks += 1
            # Tiếp tục với task tiếp theo thay vì dừng
            continue
    
    logger.info(f"Task completion summary: {completed_tasks} completed, {failed_tasks} failed")

    # Gộp tất cả file CSV
    logger.info("Starting to merge all CSV files...")
    merge_result = merge_csv_files.delay(output_dir, final_output_path, config_name, 0.7)  # 70% N/A threshold
    try:
        merge_info = merge_result.get(timeout=1800)  # timeout 30 phút
        logger.info(f"Merge file completed: {merge_info['total_rows']} rows from {merge_info['files_merged']} files")
        logger.info(f"Filtered {merge_info['filtered_rows']} NA rows")
        logger.info(f"Final file: {merge_info['final_file']}")
        return {
            "status": "success",
            "total_industries": len(industries),
            "total_companies": sum(len(links) for _, links in industries),
            "final_file": merge_info["final_file"],
            "total_rows": merge_info["total_rows"],
        }
    except Exception as e:
        logger.error(f"Error merging files: {e}")
        return {"status": "error", "message": f"Error merging files: {e}"}


def main():
    p = argparse.ArgumentParser(description="Night Crawler - Modular web crawler system")
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


if __name__ == "__main__":
    main()
