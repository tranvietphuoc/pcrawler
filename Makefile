# PCrawler - Professional Web Crawler with Phase Selection
# Makefile for easy Docker Compose management

.PHONY: help build up down logs status clean run cleanup-stats cleanup-all

# Default target
help:
	@echo "PCrawler - Professional Web Crawler with Phase Selection"
	@echo ""
	@echo "Available commands:"
	@echo "  make help              - Show this help message"
	@echo "  make build             - Build Docker images"
	@echo "  make up                - Start all services"
	@echo "  make down              - Stop all services"
	@echo "  make logs              - Show logs from all services"
	@echo "  make status            - Show current status"
	@echo "  make clean             - Clean up containers and volumes"
	@echo ""
	@echo "Crawler commands:"
	@echo "  make run               - Interactive phase and scale selection (RECOMMENDED)"
	@echo "  make export            - Run Phase 6 (export) only"
	@echo ""
	@echo "Database commands:"
	@echo "  make cleanup-stats     - Show database stats only"
	@echo "  make cleanup-all       - Full database cleanup (dedup + all tables cleanup)"
	@echo ""
	@echo "Migration:"
	@echo "  ./migrate_server.sh    - Interactive database migration script"
	@echo ""
	@echo "Examples:"
	@echo "  make run               # Interactive mode to choose phase and scale"
	@echo "  make cleanup-stats     # Show current database stats"
	@echo "  make cleanup-all       # Full database cleanup (dedup + all tables)"
	@echo "  ./migrate_server.sh    # Run database migration"

# Build Docker images
build:
	@echo "Building Docker images..."
	docker compose build

# Start all services (except crawler_app which runs on demand)
up:
	@echo "Starting background services..."
	docker compose up -d redis worker

# Stop all services
down:
	@echo "Stopping all services..."
	docker compose down

# Show logs
logs:
	@echo "Showing logs from all services..."
	docker compose logs -f

# Show logs using run_crawler script
logs-script:
	@echo "Showing logs using run_crawler script..."
	./run_crawler.sh --logs

# Show status
status:
	@echo "Current status:"
	@docker compose ps
	@echo ""
	@echo "Data directory status:"
	@if [ -d "data" ]; then \
		checkpoint_count=$$(find data -name "checkpoint_*.json" 2>/dev/null | wc -l); \
		csv_exists=""; \
		if [ -f "data/company_contacts.csv" ]; then csv_exists=" (CSV exists)"; fi; \
		echo "  - Checkpoint files: $$checkpoint_count$$csv_exists"; \
	else \
		echo "  - Data directory not found"; \
	fi

# Clean up
clean:
	@echo "Cleaning up containers and volumes..."
	docker compose down -v
	docker system prune -f

# Interactive mode
run:
	@echo "Starting interactive crawler..."
	./run_crawler.sh

# Export only (Phase 6)
export:
	@echo "Running export (Phase 6)..."
	docker compose run --rm -T crawler_app python -c 'import asyncio; from app.main import run; asyncio.run(run("1900comvn", start_phase=6))'

# Database cleanup commands
cleanup-stats:
	@echo "Showing database stats..."
	docker compose run --rm -T crawler_app python /app/app/utils/dedup_cleanup.py --stats-only

cleanup-all:
	@echo "Running full database cleanup..."
	docker compose run --rm -T crawler_app python /app/app/utils/dedup_cleanup.py

