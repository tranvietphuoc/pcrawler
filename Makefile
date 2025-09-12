# PCrawler - Professional Web Crawler with Phase Selection
# Makefile for easy Docker Compose management

.PHONY: help build up down logs status clean run-auto run-phase1 run-phase2 run-phase3 run-phase4 run-phase5 run-force-restart

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
	@echo "Crawler commands (with phase selection):"
	@echo "  make run-auto          - Run crawler with auto phase detection"
	@echo "  make run-phase1        - Run crawler starting from Phase 1"
	@echo "  make run-phase2        - Run crawler starting from Phase 2"
	@echo "  make run-phase3        - Run crawler starting from Phase 3"
	@echo "  make run-phase4        - Run crawler starting from Phase 4"
	@echo "  make run-phase5        - Run crawler starting from Phase 5"
	@echo "  make run-force-restart - Force restart from Phase 1"
	@echo ""
	@echo "Interactive mode:"
	@echo "  make run               - Interactive phase selection"
	@echo ""
	@echo "Examples:"
	@echo "  make run-auto          # Auto-detect and start from appropriate phase"
	@echo "  make run-phase2        # Start from Phase 2 (detail crawling)"
	@echo "  make run               # Interactive mode to choose phase"

# Build Docker images
build:
	@echo "Building Docker images..."
	docker-compose build

# Start all services (except crawler_app which runs on demand)
up:
	@echo "Starting background services..."
	docker-compose up -d redis worker

# Stop all services
down:
	@echo "Stopping all services..."
	docker-compose down

# Show logs
logs:
	@echo "Showing logs from all services..."
	docker-compose logs -f

# Show status
status:
	@echo "Current status:"
	@docker-compose ps
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
	docker-compose down -v
	docker system prune -f

# Interactive mode
run:
	@echo "Starting interactive crawler..."
	./run_crawler.sh

# Auto phase detection
run-auto:
	@echo "Running crawler with auto phase detection..."
	./run_crawler.sh --phase auto

# Phase 1: Links
run-phase1:
	@echo "Running crawler starting from Phase 1 (Links)..."
	./run_crawler.sh --phase 1

# Phase 2: Details
run-phase2:
	@echo "Running crawler starting from Phase 2 (Details)..."
	./run_crawler.sh --phase 2

# Phase 3: Contacts
run-phase3:
	@echo "Running crawler starting from Phase 3 (Contacts)..."
	./run_crawler.sh --phase 3

# Phase 4: Extraction
run-phase4:
	@echo "Running crawler starting from Phase 4 (Extraction)..."
	./run_crawler.sh --phase 4

# Phase 5: Export
run-phase5:
	@echo "Running crawler starting from Phase 5 (Export)..."
	./run_crawler.sh --phase 5

# Force restart
run-force-restart:
	@echo "Force restarting crawler from Phase 1..."
	./run_crawler.sh --phase 1 --force-restart