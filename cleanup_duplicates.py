#!/usr/bin/env python3
"""
Script để cleanup dữ liệu trùng lặp hiện tại trong contact_html_storage
"""

import sys
import os
import logging
import sqlite3
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database.db_manager import DatabaseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DuplicateCleanup:
    def __init__(self, db_path: str = "data/crawler.db"):
        self.db_path = db_path
        self.db_manager = DatabaseManager(db_path)
    
    def get_duplicate_stats(self) -> Dict[str, Any]:
        """Lấy thống kê về duplicates"""
        logger.info("Getting duplicate statistics...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Tổng số records
                cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
                total_records = cursor.fetchone()[0]
                
                # Số URL duy nhất
                cursor.execute("SELECT COUNT(DISTINCT url) FROM contact_html_storage WHERE url IS NOT NULL AND url != ''")
                unique_urls = cursor.fetchone()[0]
                
                # Số URL trùng lặp
                cursor.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT url, COUNT(*) as count
                        FROM contact_html_storage
                        WHERE url IS NOT NULL AND url != ''
                        GROUP BY url
                        HAVING COUNT(*) > 1
                    )
                """)
                duplicate_url_count = cursor.fetchone()[0]
                
                # Tổng số records trùng lặp
                cursor.execute("""
                    SELECT SUM(count - 1) FROM (
                        SELECT url, COUNT(*) as count
                        FROM contact_html_storage
                        WHERE url IS NOT NULL AND url != ''
                        GROUP BY url
                        HAVING COUNT(*) > 1
                    )
                """)
                total_duplicate_records = cursor.fetchone()[0] or 0
                
                stats = {
                    'total_records': total_records,
                    'unique_urls': unique_urls,
                    'duplicate_url_count': duplicate_url_count,
                    'total_duplicate_records': total_duplicate_records,
                    'duplication_rate': (total_duplicate_records / total_records * 100) if total_records > 0 else 0
                }
                
                logger.info(f"Duplicate statistics: {stats}")
                return stats
                
        except Exception as e:
            logger.error(f"Error getting duplicate stats: {e}")
            raise
    
    def cleanup_duplicates(self, keep_oldest: bool = True) -> Dict[str, int]:
        """Xóa các records trùng lặp, giữ lại record cũ nhất hoặc mới nhất"""
        logger.info(f"Starting duplicate cleanup (keep_oldest={keep_oldest})...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Tìm các URL trùng lặp
                cursor.execute("""
                    SELECT url, COUNT(*) as count
                    FROM contact_html_storage
                    WHERE url IS NOT NULL AND url != ''
                    GROUP BY url
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                """)
                
                duplicate_urls = cursor.fetchall()
                logger.info(f"Found {len(duplicate_urls)} URLs with duplicates")
                
                total_kept = 0
                total_deleted = 0
                
                for url, count in duplicate_urls:
                    logger.info(f"Processing URL: {url} ({count} duplicates)")
                    
                    # Lấy tất cả records cho URL này
                    order_clause = "ORDER BY created_at ASC" if keep_oldest else "ORDER BY created_at DESC"
                    cursor.execute(f"""
                        SELECT id, created_at
                        FROM contact_html_storage
                        WHERE url = ?
                        {order_clause}
                    """, (url,))
                    
                    records = cursor.fetchall()
                    
                    # Giữ lại record đầu tiên
                    keep_record = records[0]
                    delete_records = records[1:]
                    
                    logger.info(f"  Keeping record ID: {keep_record[0]} (created: {keep_record[1]})")
                    logger.info(f"  Deleting {len(delete_records)} duplicate records")
                    
                    # Xóa các record trùng lặp
                    for record_id, _ in delete_records:
                        cursor.execute("DELETE FROM contact_html_storage WHERE id = ?", (record_id,))
                        total_deleted += 1
                    
                    total_kept += 1
                
                conn.commit()
                
                result = {
                    'duplicate_urls_processed': len(duplicate_urls),
                    'records_kept': total_kept,
                    'records_deleted': total_deleted
                }
                
                logger.info(f"Cleanup completed: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise
    
    def run_cleanup(self, keep_oldest: bool = True, dry_run: bool = False) -> Dict[str, Any]:
        """Chạy cleanup process"""
        logger.info("=" * 80)
        logger.info("DUPLICATE CLEANUP STARTED")
        logger.info("=" * 80)
        
        # Lấy stats trước khi cleanup
        logger.info("\n" + "=" * 50)
        logger.info("STATISTICS BEFORE CLEANUP")
        logger.info("=" * 50)
        stats_before = self.get_duplicate_stats()
        
        if stats_before['duplicate_url_count'] == 0:
            logger.info("No duplicates found! Nothing to clean up.")
            return {'status': 'no_duplicates', 'stats_before': stats_before}
        
        if dry_run:
            logger.info("DRY RUN: Would delete duplicate records")
            return {'status': 'dry_run', 'stats_before': stats_before}
        
        # Cleanup duplicates
        logger.info("\n" + "=" * 50)
        logger.info("CLEANING UP DUPLICATES")
        logger.info("=" * 50)
        cleanup_result = self.cleanup_duplicates(keep_oldest=keep_oldest)
        
        # Lấy stats sau khi cleanup
        logger.info("\n" + "=" * 50)
        logger.info("STATISTICS AFTER CLEANUP")
        logger.info("=" * 50)
        stats_after = self.get_duplicate_stats()
        
        # Tính toán thay đổi
        logger.info("\n" + "=" * 50)
        logger.info("SUMMARY OF CHANGES")
        logger.info("=" * 50)
        logger.info(f"Total records: {stats_before['total_records']} → {stats_after['total_records']} ({stats_after['total_records'] - stats_before['total_records']:+d})")
        logger.info(f"Unique URLs: {stats_before['unique_urls']} → {stats_after['unique_urls']} ({stats_after['unique_urls'] - stats_before['unique_urls']:+d})")
        logger.info(f"Duplicate URLs: {stats_before['duplicate_url_count']} → {stats_after['duplicate_url_count']} ({stats_after['duplicate_url_count'] - stats_before['duplicate_url_count']:+d})")
        logger.info(f"Duplication rate: {stats_before['duplication_rate']:.1f}% → {stats_after['duplication_rate']:.1f}%")
        
        result = {
            'status': 'completed',
            'stats_before': stats_before,
            'cleanup_result': cleanup_result,
            'stats_after': stats_after
        }
        
        logger.info("\n" + "=" * 80)
        logger.info("CLEANUP COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Results: {result}")
        
        return result

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean up duplicate records in contact_html_storage')
    parser.add_argument('--keep-newest', action='store_true', help='Keep newest records instead of oldest (default: keep oldest)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--db-path', type=str, default='data/crawler.db', help='Database path')
    
    args = parser.parse_args()
    
    try:
        cleanup = DuplicateCleanup(args.db_path)
        keep_oldest = not args.keep_newest
        result = cleanup.run_cleanup(keep_oldest=keep_oldest, dry_run=args.dry_run)
        
        if result['status'] == 'completed':
            logger.info("✅ Cleanup completed successfully!")
        elif result['status'] == 'no_duplicates':
            logger.info("✅ No duplicates found!")
        elif result['status'] == 'dry_run':
            logger.info("✅ Dry run completed - no changes made")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
