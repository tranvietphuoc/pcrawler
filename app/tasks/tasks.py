import asyncio
import os
import csv
import glob
import re
import gc
import psutil
import pandas as pd
from typing import List, Dict
from .celery_app import celery_app
from app.crawler.detail_crawler import DetailCrawler
from app.crawler.backup_crawler import BackupCrawler
from app.extractor.email_extractor import EmailExtractor
from app.utils.batching_writer import safe_append_rows_csv
from config import CrawlerConfig


def clean_phone_number(phone: str) -> str:
    """Clean and format phone number to start with +, giữ format text"""
    if not phone or phone == "N/A":
        return "N/A"
    
    # Remove all non-digits
    digits_only = re.sub(r'[^\d]', '', phone)
    
    # If no digits
    if not digits_only:
        return "N/A"
    
    # If starts with 84, keep it and add +
    if digits_only.startswith('84'):
        formatted_phone = '+' + digits_only
    
    # If starts with 0, replace with 84
    elif digits_only.startswith('0'):
        formatted_phone = '+84' + digits_only[1:]
    
    # If doesn't start with 0 or 84, add 84 at the beginning
    else:
        formatted_phone = '+84' + digits_only
    
    # Ensure reasonable length (9-11 digits for +84 format)
    # +84 + 9 digits = 13 characters total (e.g., +84123456789)
    # +84 + 10 digits = 14 characters total (e.g., +841234567890)
    # +84 + 11 digits = 15 characters total (e.g., +8412345678901)
    if len(digits_only) < 9 or len(digits_only) > 11:
        return "N/A"
    
    return formatted_phone


def expand_emails(rows: List[Dict], max_emails: int = 3) -> List[Dict]:
    """
    Duplicate rows for multiple emails
    
    Args:
        rows: List of rows data
        max_emails: Maximum number of emails per row (default 3)
    
    Returns:
        List of expanded rows
    """
    expanded_rows = []
    
    for row in rows:
        emails_str = row.get("extracted_emails", "N/A")
        
        if emails_str == "N/A" or not emails_str:
            # No email, keep row
            expanded_rows.append(row)
        else:
            # Split emails
            emails = [email.strip() for email in emails_str.split(";") if email.strip()]
            
            # Limit number of emails
            emails = emails[:max_emails]
            
            if len(emails) == 1:
                # Only one email, keep row
                row["extracted_emails"] = emails[0]
                expanded_rows.append(row)
            else:
                # Multiple emails, create multiple rows
                for email in emails:
                    new_row = row.copy()
                    new_row["extracted_emails"] = email
                    expanded_rows.append(new_row)
    
    return expanded_rows


def filter_na_rows(rows: List[Dict], max_na_percentage: float = 0.7) -> List[Dict]:
    """
    Filter out rows with too many N/A values
    
    Args:
        rows: List of rows data
        max_na_percentage: Maximum allowed N/A percentage (0.0 - 1.0). Default 70%
    
    Returns:
        List of filtered rows
    """
    if not rows:
        return rows
    
    filtered_rows = []
    total_fields = len(rows[0]) if rows else 0
    
    for row in rows:
        # Count number of fields with N/A value
        na_count = sum(1 for value in row.values() if value == "N/A" or value is None)
        na_percentage = na_count / total_fields if total_fields > 0 else 0
        
        # Keep only rows with N/A percentage below threshold
        if na_percentage <= max_na_percentage:
            filtered_rows.append(row)
    
    return filtered_rows


@celery_app.task(name="crawl.details_extract_write")
def crawl_details_extract_write(
    links: List[str],
    industry_name: str,
    output_dir: str,
    task_id: str,
    config_name: str = "default",
    max_concurrent_pages: int = None,
    write_batch_size: int = None,
) -> dict:
    """Crawl details + extract email + write CSV for each task."""
    # Memory monitoring
    process = psutil.Process()
    mem_before = process.memory_info().rss // (1024 * 1024)
    print(f"[MEMORY][Task {task_id}] start: {mem_before} MB")

    # Load config
    config = CrawlerConfig(config_name)
    max_concurrent_pages = max_concurrent_pages or config.processing_config["max_concurrent_pages"]
    write_batch_size = write_batch_size or config.processing_config["write_batch_size"]
    
    # Create file for this task
    task_file = os.path.join(output_dir, f"task_{task_id}.csv")
    
    async def _run():
        detail = DetailCrawler(config=config, max_concurrent_pages=max_concurrent_pages)
        
        # Tạo EmailExtractor với error handling
        extractor = None
        try:
            extractor = EmailExtractor(config=config)
        except Exception as e:
            print(f"[ERROR] Failed to create EmailExtractor: {e}")
            # Fallback: crawl without email extraction
            extractor = None
        
        try:
            rows = await detail.crawl_company_batch(links)
        except Exception as e:
            print(f"[ERROR] Failed to crawl company batch: {e}")
            return {
                "task_id": task_id,
                "file_path": task_file,
                "rows_processed": 0,
                "status": "error",
                "error": str(e)
            }

        pending = []
        for d in rows:
            # Add industry name only
            d["industry_name"] = industry_name

            # Clean phone number
            d["phone"] = clean_phone_number(d.get("phone", "N/A"))

            # Extract email (prioritize FB) - với error handling
            emails = None
            email_src = "N/A"
            
            if extractor:  # Chỉ extract email nếu extractor hoạt động
                try:
                    if d.get("facebook") and d["facebook"] != "N/A":
                        e1 = await extractor.from_facebook(d["facebook"])
                        if e1:
                            emails, email_src = e1, "Facebook"
                    if (
                        not emails
                        and d.get("website")
                        and d["website"] != "N/A"
                    ):
                        e2 = await extractor.from_website(d["website"])
                        if e2:
                            emails, email_src = e2, "Website"
                except Exception as e:
                    print(f"[WARNING] Email extraction failed for {d.get('name', 'Unknown')}: {e}")
                    # Continue without email extraction
                    emails = None
                    email_src = "N/A"

            d["extracted_emails"] = "; ".join(emails) if emails else "N/A"
            d["email_source"] = email_src

            pending.append(d)
            if len(pending) >= write_batch_size:
                # Expand emails before writing
                expanded_pending = expand_emails(pending)
                safe_append_rows_csv(task_file, expanded_pending, config.get_fieldnames())
                pending.clear()

        if pending:
            # Expand emails before writing
            expanded_pending = expand_emails(pending)
            safe_append_rows_csv(task_file, expanded_pending, config.get_fieldnames())
            
        # Cleanup EmailExtractor
        if extractor:
            try:
                # Sử dụng async cleanup nếu có thể
                if hasattr(extractor, 'async_cleanup'):
                    await extractor.async_cleanup()
                else:
                    extractor.cleanup()
            except Exception as e:
                print(f"[WARNING] EmailExtractor cleanup failed: {e}")
                
        # Memory cleanup
        gc.collect()
        mem_after = process.memory_info().rss // (1024 * 1024)
        print(f"[MEMORY][Task {task_id}] after GC: {mem_after} MB (freed ~{max(0, mem_before - mem_after)} MB)")

        return {
            "task_id": task_id,
            "file_path": task_file,
            "rows_processed": len(rows),
            "status": "completed"
        }

    return asyncio.run(_run())


@celery_app.task(name="merge.csv_files")
def merge_csv_files(output_dir: str, final_output_path: str, config_name: str = "default", max_na_percentage: float = 0.7) -> dict:
    """Merge all CSV files from all tasks into a single file and filter out N/A rows."""
    
    # Load config
    config = CrawlerConfig(config_name)
    
    # Find all task_*.csv files
    task_files = glob.glob(os.path.join(output_dir, "task_*.csv"))
    
    if not task_files:
        return {
            "status": "error",
            "message": "Not found any task file to merge"
        }
    
    total_rows = 0
    filtered_rows = 0
    
    # Create directory for final output file
    os.makedirs(os.path.dirname(final_output_path) or ".", exist_ok=True)
    
    with open(final_output_path, 'w', newline='', encoding='utf-8-sig') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=config.get_fieldnames())
        writer.writeheader()
        
        for task_file in sorted(task_files):
            try:
                with open(task_file, 'r', newline='', encoding='utf-8-sig') as infile:
                    reader = csv.DictReader(infile)
                    file_rows = list(reader)
                    
                    # Filter out rows with N/A
                    original_count = len(file_rows)
                    filtered_file_rows = filter_na_rows(file_rows, max_na_percentage)
                    filtered_count = len(filtered_file_rows)
                    
                    # Expand emails
                    expanded_file_rows = expand_emails(filtered_file_rows, 3)  # max 3 emails
                    expanded_count = len(expanded_file_rows)
                    
                    # Write filtered and expanded rows
                    for row in expanded_file_rows:
                        writer.writerow(row)
                        total_rows += 1
                    
                    filtered_rows += (original_count - filtered_count)
                    expanded_rows = expanded_count - filtered_count
                    print(f"Merged file {task_file}: {filtered_count}/{original_count} rows kept (filtered {original_count - filtered_count} NA rows, expanded {expanded_rows} email rows)")
                        
                # Delete task file after merging
                os.remove(task_file)
                print(f"Deleted file: {task_file}")
                
            except Exception as e:
                print(f"Error processing file {task_file}: {e}")
                continue
    
    return {
        "status": "completed",
        "final_file": final_output_path,
        "total_rows": total_rows,
        "filtered_rows": filtered_rows,
        "files_merged": len(task_files),
        "max_na_percentage": max_na_percentage,
        "max_emails": 3
    }


@celery_app.task(name="backup.crawl_na_emails", bind=True)
def backup_crawl_na_emails(self, merged_file_path: str, batch_size: int = 10):
    """
    Backup crawl các dòng có extracted_emails = N/A
    """
    try:
        config = CrawlerConfig()
        backup_crawler = BackupCrawler(config)
        
        # Tạo event loop mới cho task này
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Load file và filter N/A rows
        na_rows = backup_crawler.load_merged_file(merged_file_path)
        
        if na_rows.empty:
            print("No N/A emails found, backup crawl completed")
            return {
                'status': 'completed',
                'message': 'No N/A emails found',
                'total_rows': 0,
                'updated_rows': 0
            }
        
        print(f"Starting backup crawl for {len(na_rows)} N/A rows")
        
        # Process theo batch
        backup_results = []
        total_rows = len(na_rows)
        
        for i in range(0, total_rows, batch_size):
            batch = na_rows.iloc[i:i + batch_size]
            
            # Process batch
            batch_results = []
            for _, row in batch.iterrows():
                try:
                    result = loop.run_until_complete(backup_crawler.deep_crawl_emails(row))
                    batch_results.append(result)
                    
                    # Update progress
                    self.update_state(
                        state='PROGRESS',
                        meta={
                            'current': i + len(batch_results),
                            'total': total_rows,
                            'status': f'Processing {row["name"]}'
                        }
                    )
                    
                except Exception as e:
                    print(f"Error processing {row['name']}: {e}")
                    continue
            
            backup_results.extend(batch_results)
            
            # Log progress
            print(f"Processed batch {i//batch_size + 1}: {len(batch_results)} results")
        
        # Update merged file (ghi đè file gốc)
        updated_file = backup_crawler.update_merged_file(merged_file_path, backup_results)
        
        # Count updated rows (chỉ đếm những dòng thực sự được update từ N/A)
        updated_count = len([r for r in backup_results if r['extracted_emails'] != 'N/A'])
        
        print(f"Backup crawl completed: {updated_count}/{total_rows} N/A rows updated")
        print(f"Updated file: {updated_file}")
        
        return {
            'status': 'completed',
            'message': f'Backup crawl completed: {updated_count}/{total_rows} N/A rows updated',
            'total_rows': total_rows,
            'updated_rows': updated_count,
            'updated_file': updated_file
        }
        
    except Exception as e:
        print(f"Backup crawl failed: {e}")
        return {
            'status': 'failed',
            'message': str(e),
            'total_rows': 0,
            'updated_rows': 0
        }
    finally:
        # Cleanup event loop
        try:
            if 'loop' in locals():
                loop.close()
        except Exception as cleanup_error:
            print(f"Event loop cleanup error: {cleanup_error}")


@celery_app.task(name="backup.check_and_crawl")
def check_and_backup_crawl(merged_file_path: str = None):
    """
    Check nếu có file merged, load và backup crawl N/A emails
    Nếu chưa có, chạy crawl từ đầu
    """
    try:
        # Tìm file merged
        if not merged_file_path:
            config = CrawlerConfig()
            merged_file_path = config.output_config["final_output"]
        
        if not os.path.exists(merged_file_path):
            print(f"Merged file not found: {merged_file_path}")
            print("Starting full crawl from beginning...")
            
            # Import và chạy full crawl
            from app.main import run
            result = asyncio.run(run())
            return {
                'status': 'full_crawl_completed',
                'message': 'Full crawl completed',
                'result': result
            }
        
        # File đã tồn tại, backup crawl N/A emails
        print(f"Found merged file: {merged_file_path}")
        print("Starting backup crawl for N/A emails...")
        
        # Chạy backup crawl task và chờ kết quả
        backup_task = backup_crawl_na_emails.delay(merged_file_path)
        result = backup_task.get(timeout=3600)  # timeout 1 giờ
        
        return {
            'status': 'backup_crawl_completed',
            'message': 'Backup crawl completed',
            'result': result,
            'merged_file': merged_file_path
        }
        
    except Exception as e:
        print(f"Check and backup crawl failed: {e}")
        return {
            'status': 'failed',
            'message': str(e)
        }
