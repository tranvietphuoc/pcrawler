#!/usr/bin/env python3
"""
Script để gộp file CSV thủ công.
Sử dụng khi cần gộp lại file sau khi quá trình crawl bị gián đoạn.
"""

import os
import sys
import glob
import csv
import argparse
import re
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


def filter_na_rows(rows, max_na_percentage: float = 0.7):
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


def expand_emails(rows, max_emails: int = 3):
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


def manual_merge(output_dir: str, final_output_path: str, config_name: str = "default", max_na_percentage: float = 0.7, max_emails: int = 3):
    """Manual merge CSV files without Celery and filter out N/A rows."""
    
    # Load config
    config = CrawlerConfig(config_name)
    
    # Find all task_*.csv files
    task_files = glob.glob(os.path.join(output_dir, "task_*.csv"))
    
    if not task_files:
        print(f"No task file found in directory: {output_dir}")
        return
    
    print(f"Found {len(task_files)} task files:")
    for f in sorted(task_files):
        print(f"  - {f}")
    
    total_rows = 0
    filtered_rows = 0
    expanded_rows = 0
    
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
                    expanded_file_rows = expand_emails(filtered_file_rows, max_emails)
                    expanded_count = len(expanded_file_rows)
                    
                    # Write filtered and expanded rows
                    for row in expanded_file_rows:
                        writer.writerow(row)
                        total_rows += 1
                    
                    filtered_rows += (original_count - filtered_count)
                    expanded_rows += (expanded_count - filtered_count)
                    print(f"Merged file {task_file}: {filtered_count}/{original_count} rows kept (filtered {original_count - filtered_count} NA rows, expanded {expanded_count - filtered_count} email rows)")
                        
                # Ask if want to delete task file
                if input(f"Delete file {task_file}? (y/N): ").lower() == 'y':
                    os.remove(task_file)
                    print(f"Deleted file: {task_file}")
                
            except Exception as e:
                print(f"Error processing file {task_file}: {e}")
                continue
    
    print(f"\nMerge file completed!")
    print(f"Total rows: {total_rows}")
    print(f"Filtered rows: {filtered_rows}")
    print(f"Expanded rows: {expanded_rows}")
    print(f"Max N/A percentage: {max_na_percentage * 100}%")
    print(f"Max emails per row: {max_emails}")
    print(f"Final output file: {final_output_path}")


def main():
    parser = argparse.ArgumentParser(description="Manual merge CSV files")
    parser.add_argument("--output-dir", required=True, help="Directory containing task files")
    parser.add_argument("--final-output", required=True, help="Final output file")
    parser.add_argument("--config", default="default", help="Configuration name (default, 1900comvn, example)")
    parser.add_argument("--auto-delete", action="store_true", help="Automatically delete task file after merge")
    parser.add_argument("--max-na-percentage", type=float, default=0.7, help="Rate of N/A allowed (0.0-1.0, default 0.7)")
    parser.add_argument("--max-emails", type=int, default=3, help="Max emails per row (default 3)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        print(f"Directory does not exist: {args.output_dir}")
        return
    
    manual_merge(args.output_dir, args.final_output, args.config, args.max_na_percentage, args.max_emails)

if __name__ == "__main__":
    main()
