#!/bin/bash

# Migration script for server deployment
# This script applies unique constraints to the database

set -e  # Exit on any error

echo "=========================================="
echo "PCrawler Database Migration Script"
echo "=========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose > /dev/null 2>&1; then
    echo "ERROR: docker-compose is not installed or not in PATH."
    exit 1
fi

# Check if database exists
if [ ! -f "data/crawler.db" ]; then
    echo "ERROR: Database file 'data/crawler.db' not found."
    echo "Please make sure you're in the correct directory and the database exists."
    exit 1
fi

echo "Current database stats before migration:"
make cleanup-stats

echo ""
echo "Running dry-run migration to check what will be changed..."
docker-compose run --rm -T crawler_app python /app/app/database/migrate_unique_constraints.py --dry-run

echo ""
read -p "Do you want to proceed with the migration? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Migration cancelled."
    exit 0
fi

echo ""
echo "Starting migration..."
docker-compose run --rm -T crawler_app python /app/app/database/migrate_unique_constraints.py

echo ""
echo "Migration completed! Current database stats after migration:"
make cleanup-stats

echo ""
echo "=========================================="
echo "Migration Summary:"
echo "=========================================="
echo "✓ Added UNIQUE constraint to detail_html_storage.company_url"
echo "✓ Added UNIQUE constraint to contact_html_storage(url, url_type)"
echo "✓ Removed duplicate records (kept latest)"
echo "✓ Recreated indexes for better performance"
echo "✓ Updated foreign key references"
echo ""
echo "The crawler will now automatically skip duplicate URLs"
echo "and prevent duplicate crawling to save bandwidth and storage."
echo "=========================================="
