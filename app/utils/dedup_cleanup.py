#!/usr/bin/env python3
"""
Script để deduplicate records trong detail_html_storage và cleanup contact_html_storage
"""

import sys
import os
import logging
from typing import List, Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.database.db_manager import DatabaseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseCleanup:
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def deduplicate_detail_html_storage(self) -> Dict[str, int]:
        """
        Deduplicate records trong detail_html_storage theo company_url
        Giữ lại record đầu tiên, xóa các record trùng lặp
        """
        logger.info("Starting deduplication of detail_html_storage...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Tìm các record trùng lặp
                cursor.execute("""
                    SELECT company_url, COUNT(*) as count
                    FROM detail_html_storage
                    WHERE company_url IS NOT NULL AND company_url != ''
                    GROUP BY company_url
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                """)
                
                duplicate_urls = cursor.fetchall()
                logger.info(f"Found {len(duplicate_urls)} URLs with duplicates")
                
                total_duplicates = 0
                total_kept = 0
                total_deleted = 0
                
                for url, count in duplicate_urls:
                    logger.info(f"Processing URL: {url} ({count} duplicates)")
                    
                    # Lấy tất cả records cho URL này
                    cursor.execute("""
                        SELECT id, created_at
                        FROM detail_html_storage
                        WHERE company_url = ?
                        ORDER BY created_at ASC
                    """, (url,))
                    
                    records = cursor.fetchall()
                    
                    # Giữ lại record đầu tiên (oldest)
                    keep_record = records[0]
                    delete_records = records[1:]
                    
                    logger.info(f"  Keeping record ID: {keep_record[0]} (created: {keep_record[1]})")
                    logger.info(f"  Deleting {len(delete_records)} duplicate records")
                    
                    # Xóa các record trùng lặp
                    for record_id, _ in delete_records:
                        cursor.execute("DELETE FROM detail_html_storage WHERE id = ?", (record_id,))
                        total_deleted += 1
                    
                    total_kept += 1
                    total_duplicates += count
                
                conn.commit()
                
                result = {
                    'duplicate_urls': len(duplicate_urls),
                    'total_duplicates': total_duplicates,
                    'total_kept': total_kept,
                    'total_deleted': total_deleted
                }
                
                logger.info(f"Deduplication completed: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Error during deduplication: {e}")
            raise
    
    def cleanup_contact_html_storage(self) -> Dict[str, int]:
        """
        Xóa tất cả records trong contact_html_storage
        """
        logger.info("Starting cleanup of contact_html_storage...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Đếm số records trước khi xóa
                cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
                total_records = cursor.fetchone()[0]
                
                logger.info(f"Found {total_records} records in contact_html_storage")
                
                if total_records > 0:
                    # Xóa tất cả records
                    cursor.execute("DELETE FROM contact_html_storage")
                    conn.commit()
                    
                    logger.info(f"Deleted {total_records} records from contact_html_storage")
                else:
                    logger.info("No records to delete in contact_html_storage")
                
                result = {
                    'total_deleted': total_records
                }
                
                return result
                
        except Exception as e:
            logger.error(f"Error during contact_html_storage cleanup: {e}")
            raise
    
    def get_database_stats(self) -> Dict[str, int]:
        """
        Lấy thống kê database
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Đếm records trong các bảng
                cursor.execute("SELECT COUNT(*) FROM detail_html_storage")
                detail_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
                contact_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM company_details")
                company_details_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM email_extraction")
                email_count = cursor.fetchone()[0]
                
                stats = {
                    'detail_html_storage': detail_count,
                    'contact_html_storage': contact_count,
                    'company_details': company_details_count,
                    'email_extraction': email_count
                }
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            raise
    
    def run_cleanup(self, dedup_detail: bool = True, cleanup_contact: bool = True) -> Dict[str, Any]:
        """
        Chạy cleanup process
        """
        logger.info("=" * 80)
        logger.info("DATABASE CLEANUP STARTED")
        logger.info("=" * 80)
        
        # Lấy stats trước khi cleanup
        logger.info("Database stats BEFORE cleanup:")
        stats_before = self.get_database_stats()
        for table, count in stats_before.items():
            logger.info(f"  {table}: {count} records")
        
        results = {}
        
        # Deduplicate detail_html_storage
        if dedup_detail:
            logger.info("\n" + "=" * 50)
            logger.info("DEDUPLICATING DETAIL_HTML_STORAGE")
            logger.info("=" * 50)
            results['deduplication'] = self.deduplicate_detail_html_storage()
        
        # Cleanup contact_html_storage
        if cleanup_contact:
            logger.info("\n" + "=" * 50)
            logger.info("CLEANING UP CONTACT_HTML_STORAGE")
            logger.info("=" * 50)
            results['contact_cleanup'] = self.cleanup_contact_html_storage()
        
        # Lấy stats sau khi cleanup
        logger.info("\n" + "=" * 50)
        logger.info("Database stats AFTER cleanup:")
        stats_after = self.get_database_stats()
        for table, count in stats_after.items():
            logger.info(f"  {table}: {count} records")
        
        # Tính toán thay đổi
        logger.info("\n" + "=" * 50)
        logger.info("SUMMARY OF CHANGES:")
        logger.info("=" * 50)
        for table in stats_before:
            before = stats_before[table]
            after = stats_after[table]
            change = after - before
            logger.info(f"  {table}: {before} → {after} ({change:+d})")
        
        results['stats_before'] = stats_before
        results['stats_after'] = stats_after
        
        logger.info("\n" + "=" * 80)
        logger.info("DATABASE CLEANUP COMPLETED")
        logger.info("=" * 80)
        
        return results

def main():
    """
    Main function
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Database cleanup and deduplication script')
    parser.add_argument('--dedup-detail', action='store_true', default=True,
                       help='Deduplicate detail_html_storage (default: True)')
    parser.add_argument('--no-dedup-detail', action='store_true',
                       help='Skip deduplication of detail_html_storage')
    parser.add_argument('--cleanup-contact', action='store_true', default=True,
                       help='Cleanup contact_html_storage (default: True)')
    parser.add_argument('--no-cleanup-contact', action='store_true',
                       help='Skip cleanup of contact_html_storage')
    parser.add_argument('--stats-only', action='store_true',
                       help='Show database stats only, no cleanup')
    
    args = parser.parse_args()
    
    # Determine actions
    dedup_detail = args.dedup_detail and not args.no_dedup_detail
    cleanup_contact = args.cleanup_contact and not args.no_cleanup_contact
    
    if args.stats_only:
        dedup_detail = False
        cleanup_contact = False
    
    try:
        cleanup = DatabaseCleanup()
        
        if args.stats_only:
            logger.info("Showing database stats only...")
            stats = cleanup.get_database_stats()
            logger.info("Current database stats:")
            for table, count in stats.items():
                logger.info(f"  {table}: {count} records")
        else:
            results = cleanup.run_cleanup(
                dedup_detail=dedup_detail,
                cleanup_contact=cleanup_contact
            )
            
            logger.info("Cleanup completed successfully!")
            logger.info(f"Results: {results}")
    
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
