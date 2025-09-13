#!/usr/bin/env python3
"""
Migration script to add unique constraints to prevent duplicate crawling
Usage: python migrate_unique_constraints.py [--db-path PATH] [--dry-run]
"""
import sqlite3
import logging
import os
import argparse
from typing import List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def migrate_database(db_path: str = "data/crawler.db", dry_run: bool = False):
    """Add unique constraints to prevent duplicate crawling"""
    
    if not os.path.exists(db_path):
        logger.error(f"Database not found: {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        logger.info("Starting migration: Adding unique constraints...")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        # 1. Check for duplicate company_url in detail_html_storage
        logger.info("Checking for duplicate company_url in detail_html_storage...")
        cursor.execute("""
            SELECT company_url, COUNT(*) as count 
            FROM detail_html_storage 
            GROUP BY company_url 
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        
        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate company_url entries:")
            for url, count in duplicates[:10]:  # Show first 10
                logger.warning(f"  {url}: {count} duplicates")
            
            if not dry_run:
                # Remove duplicates, keep the latest one
                logger.info("Removing duplicate entries (keeping latest)...")
                cursor.execute("""
                    DELETE FROM detail_html_storage 
                    WHERE id NOT IN (
                        SELECT MAX(id) 
                        FROM detail_html_storage 
                        GROUP BY company_url
                    )
                """)
                removed_count = cursor.rowcount
                logger.info(f"Removed {removed_count} duplicate entries")
        else:
            logger.info("No duplicate company_url entries found")
        
        # 2. Check for duplicate url in contact_html_storage
        logger.info("Checking for duplicate url in contact_html_storage...")
        cursor.execute("""
            SELECT url, url_type, COUNT(*) as count 
            FROM contact_html_storage 
            GROUP BY url, url_type 
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        
        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate url entries:")
            for url, url_type, count in duplicates[:10]:  # Show first 10
                logger.warning(f"  {url} ({url_type}): {count} duplicates")
            
            if not dry_run:
                # Remove duplicates, keep the latest one
                logger.info("Removing duplicate entries (keeping latest)...")
                cursor.execute("""
                    DELETE FROM contact_html_storage 
                    WHERE id NOT IN (
                        SELECT MAX(id) 
                        FROM contact_html_storage 
                        GROUP BY url, url_type
                    )
                """)
                removed_count = cursor.rowcount
                logger.info(f"Removed {removed_count} duplicate entries")
        else:
            logger.info("No duplicate url entries found")
        
        if not dry_run:
            # 3. Create new tables with unique constraints
            logger.info("Creating new tables with unique constraints...")
            
            # Create new detail_html_storage with unique constraint
            cursor.execute("""
                CREATE TABLE detail_html_storage_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL,
                    company_url TEXT NOT NULL UNIQUE,
                    industry TEXT,
                    html_content TEXT NOT NULL,
                    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create new contact_html_storage without unique constraint
            cursor.execute("""
                CREATE TABLE contact_html_storage_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    url_type TEXT NOT NULL,
                    html_content TEXT NOT NULL,
                    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 4. Copy data from old tables to new tables
            logger.info("Copying data to new tables...")
            
            cursor.execute("""
                INSERT INTO detail_html_storage_new 
                SELECT * FROM detail_html_storage
            """)
            
            cursor.execute("""
                INSERT INTO contact_html_storage_new 
                SELECT * FROM contact_html_storage
            """)
            
            # 5. Drop old tables and rename new tables
            logger.info("Replacing old tables with new ones...")
            
            cursor.execute("DROP TABLE detail_html_storage")
            cursor.execute("DROP TABLE contact_html_storage")
            
            cursor.execute("ALTER TABLE detail_html_storage_new RENAME TO detail_html_storage")
            cursor.execute("ALTER TABLE contact_html_storage_new RENAME TO contact_html_storage")
            
            # 6. Recreate indexes
            logger.info("Recreating indexes...")
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_detail_html_status ON detail_html_storage(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_detail_html_company ON detail_html_storage(company_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_detail_html_url ON detail_html_storage(company_url)")
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_status ON contact_html_storage(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_company ON contact_html_storage(company_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_type ON contact_html_storage(url_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_html_url ON contact_html_storage(url)")
            
            # 7. Update foreign key references
            logger.info("Updating foreign key references...")
            
            # Update company_details to reference new detail_html_storage
            cursor.execute("""
                CREATE TABLE company_details_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    detail_html_id INTEGER NOT NULL,
                    company_name TEXT NOT NULL,
                    company_url TEXT NOT NULL,
                    address TEXT,
                    phone TEXT,
                    website TEXT,
                    facebook TEXT,
                    linkedin TEXT,
                    tiktok TEXT,
                    youtube TEXT,
                    instagram TEXT,
                    created_year TEXT,
                    revenue TEXT,
                    scale TEXT,
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (detail_html_id) REFERENCES detail_html_storage (id)
                )
            """)
            
            cursor.execute("INSERT INTO company_details_new SELECT * FROM company_details")
            cursor.execute("DROP TABLE company_details")
            cursor.execute("ALTER TABLE company_details_new RENAME TO company_details")
            
            # Update email_extraction to reference new contact_html_storage
            cursor.execute("""
                CREATE TABLE email_extraction_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_html_id INTEGER NOT NULL,
                    company_name TEXT NOT NULL,
                    extracted_emails TEXT,
                    email_source TEXT,
                    extraction_method TEXT,
                    confidence_score REAL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (contact_html_id) REFERENCES contact_html_storage (id)
                )
            """)
            
            cursor.execute("INSERT INTO email_extraction_new SELECT * FROM email_extraction")
            cursor.execute("DROP TABLE email_extraction")
            cursor.execute("ALTER TABLE email_extraction_new RENAME TO email_extraction")
            
            # 8. Commit changes
            conn.commit()
            
            # 9. Verify migration
            logger.info("Verifying migration...")
            
            cursor.execute("SELECT COUNT(*) FROM detail_html_storage")
            detail_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM contact_html_storage")
            contact_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM company_details")
            company_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM email_extraction")
            email_count = cursor.fetchone()[0]
            
            logger.info(f"Migration completed successfully!")
            logger.info(f"Records after migration:")
            logger.info(f"  detail_html_storage: {detail_count}")
            logger.info(f"  contact_html_storage: {contact_count}")
            logger.info(f"  company_details: {company_count}")
            logger.info(f"  email_extraction: {email_count}")
            
            # 10. Test unique constraints
            logger.info("Testing unique constraints...")
            
            # Test detail_html_storage unique constraint
            cursor.execute("SELECT COUNT(*) FROM detail_html_storage")
            detail_count = cursor.fetchone()[0]
            
            if detail_count > 0:
                try:
                    # Try to insert duplicate company_url
                    cursor.execute("""
                        INSERT INTO detail_html_storage (company_name, company_url, html_content)
                        SELECT company_name, company_url, html_content FROM detail_html_storage LIMIT 1
                    """)
                    logger.error("ERROR: Unique constraint not working for detail_html_storage!")
                except sqlite3.IntegrityError:
                    logger.info("✓ Unique constraint working for detail_html_storage")
            else:
                logger.info("✓ detail_html_storage: Unique constraint applied (table empty, cannot test)")
            
            # Skip testing contact_html_storage unique constraint (not implemented)
            logger.info("✓ contact_html_storage: No unique constraint (as requested)")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate database to add unique constraints")
    parser.add_argument("--db-path", default="data/crawler.db", help="Database file path")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    
    args = parser.parse_args()
    
    success = migrate_database(args.db_path, args.dry_run)
    if success:
        logger.info("Migration completed successfully!")
    else:
        logger.error("Migration failed!")
        exit(1)
