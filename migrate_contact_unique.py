#!/usr/bin/env python3
"""
Script để migrate database và thêm unique constraint cho contact_html_storage
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

class ContactMigration:
    def __init__(self, db_path: str = "data/crawler.db"):
        self.db_path = db_path
        self.db_manager = DatabaseManager(db_path)
    
    def analyze_duplicates(self) -> Dict[str, Any]:
        """Phân tích các URL trùng lặp trong contact_html_storage"""
        logger.info("Analyzing duplicates in contact_html_storage...")
        
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
                
                total_duplicates = 0
                for url, count in duplicate_urls:
                    total_duplicates += count
                    logger.info(f"  {url}: {count} duplicates")
                
                # Tổng số records
                cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
                total_records = cursor.fetchone()[0]
                
                # Số URL duy nhất
                cursor.execute("SELECT COUNT(DISTINCT url) FROM contact_html_storage WHERE url IS NOT NULL AND url != ''")
                unique_urls = cursor.fetchone()[0]
                
                result = {
                    'total_records': total_records,
                    'unique_urls': unique_urls,
                    'duplicate_urls_count': len(duplicate_urls),
                    'total_duplicates': total_duplicates,
                    'duplicate_urls': duplicate_urls[:10]  # Top 10
                }
                
                logger.info(f"Analysis completed: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            raise
    
    def deduplicate_contact_storage(self) -> Dict[str, int]:
        """Deduplicate records trong contact_html_storage theo URL"""
        logger.info("Starting deduplication of contact_html_storage...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Tìm các record trùng lặp
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
                
                total_duplicates = 0
                total_kept = 0
                total_deleted = 0
                
                for url, count in duplicate_urls:
                    logger.info(f"Processing URL: {url} ({count} duplicates)")
                    
                    # Lấy tất cả records cho URL này
                    cursor.execute("""
                        SELECT id, created_at
                        FROM contact_html_storage
                        WHERE url = ?
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
                        cursor.execute("DELETE FROM contact_html_storage WHERE id = ?", (record_id,))
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
    
    def add_unique_constraint(self) -> bool:
        """Thêm unique constraint cho cột url trong contact_html_storage"""
        logger.info("Adding unique constraint to contact_html_storage.url...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Tạo bảng mới với unique constraint
                cursor.execute("""
                    CREATE TABLE contact_html_storage_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_name TEXT NOT NULL,
                        url TEXT NOT NULL UNIQUE,
                        url_type TEXT NOT NULL,
                        html_content TEXT NOT NULL,
                        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status TEXT DEFAULT 'pending',
                        retry_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Copy data từ bảng cũ
                cursor.execute("""
                    INSERT INTO contact_html_storage_new 
                    SELECT * FROM contact_html_storage
                """)
                
                # Drop bảng cũ và rename bảng mới
                cursor.execute("DROP TABLE contact_html_storage")
                cursor.execute("ALTER TABLE contact_html_storage_new RENAME TO contact_html_storage")
                
                # Tạo lại indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_status ON contact_html_storage(status)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_company ON contact_html_storage(company_name)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_type ON contact_html_storage(url_type)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_url ON contact_html_storage(url)")
                
                conn.commit()
                
                logger.info("Unique constraint added successfully!")
                return True
                
        except Exception as e:
            logger.error(f"Error adding unique constraint: {e}")
            raise
    
    def run_migration(self, dry_run: bool = False) -> Dict[str, Any]:
        """Chạy migration process"""
        logger.info("=" * 80)
        logger.info("CONTACT STORAGE MIGRATION STARTED")
        logger.info("=" * 80)
        
        # Phân tích duplicates
        logger.info("\n" + "=" * 50)
        logger.info("ANALYZING DUPLICATES")
        logger.info("=" * 50)
        analysis = self.analyze_duplicates()
        
        if analysis['duplicate_urls_count'] == 0:
            logger.info("No duplicates found! Adding unique constraint directly...")
            if not dry_run:
                self.add_unique_constraint()
            return {'status': 'no_duplicates', 'analysis': analysis}
        
        if dry_run:
            logger.info("DRY RUN: Would deduplicate and add unique constraint")
            return {'status': 'dry_run', 'analysis': analysis}
        
        # Deduplicate
        logger.info("\n" + "=" * 50)
        logger.info("DEDUPLICATING RECORDS")
        logger.info("=" * 50)
        dedup_result = self.deduplicate_contact_storage()
        
        # Thêm unique constraint
        logger.info("\n" + "=" * 50)
        logger.info("ADDING UNIQUE CONSTRAINT")
        logger.info("=" * 50)
        constraint_added = self.add_unique_constraint()
        
        # Kiểm tra kết quả
        logger.info("\n" + "=" * 50)
        logger.info("VERIFICATION")
        logger.info("=" * 50)
        final_analysis = self.analyze_duplicates()
        
        result = {
            'status': 'completed',
            'analysis_before': analysis,
            'deduplication': dedup_result,
            'constraint_added': constraint_added,
            'analysis_after': final_analysis
        }
        
        logger.info("\n" + "=" * 80)
        logger.info("MIGRATION COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Results: {result}")
        
        return result

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate contact_html_storage to add unique constraint')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--db-path', type=str, default='data/crawler.db', help='Database path')
    
    args = parser.parse_args()
    
    try:
        migration = ContactMigration(args.db_path)
        result = migration.run_migration(dry_run=args.dry_run)
        
        if result['status'] == 'completed':
            logger.info("✅ Migration completed successfully!")
        elif result['status'] == 'no_duplicates':
            logger.info("✅ No duplicates found, unique constraint added!")
        elif result['status'] == 'dry_run':
            logger.info("✅ Dry run completed - no changes made")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
