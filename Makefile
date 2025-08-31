.PHONY: help docker-build docker-up docker-down docker-logs crawl docker-crawl docker-crawl-1900 docker-scale-1 docker-scale-2 docker-merge

# Default target
help:
	@echo "PCrawler - Simple Commands:"
	@echo ""
	@echo "Docker Setup:"
	@echo "  docker-build   Build Docker images"
	@echo "  docker-up      Start services (Redis + Worker)"
	@echo "  docker-down    Stop all services"
	@echo "  docker-logs    Show logs"
	@echo ""
	@echo "Crawling:"
	@echo "  crawl          Start crawling (local)"
	@echo "  docker-crawl   Start crawling (Docker)"
	@echo "  docker-scale-1 Safe mode (1 worker)"
	@echo "  docker-scale-2 Fast mode (2 workers)"
	@echo ""
	@echo "Manual:"
	@echo "  docker-merge   Merge CSV files"

# Local crawling
crawl:
	@echo "Starting crawling (local)..."
	uv run python -m app.main crawl --config 1900comvn

# Docker
docker-build:
	@echo "Building Docker images..."
	docker-compose build

docker-up:
	@echo "Starting Docker services..."
	docker-compose up

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down

docker-logs:
	@echo "Showing Docker logs..."
	docker-compose logs -f

# Docker scaling
docker-scale-1:
	@echo "Safe mode (1 worker) - Low risk, slower"
	docker-compose up --scale worker=1 -d

docker-scale-2:
	@echo "Fast mode (2 workers) - Balanced speed/risk"
	docker-compose up --scale worker=2 -d

# Docker crawling
docker-crawl:
	@echo "Starting crawling (Docker)..."
	docker-compose run --rm app python -m app.main crawl --config 1900comvn

# Manual merge
docker-merge:
	@echo "Merging CSV files..."
	docker-compose run --rm app python merge_files.py --output-dir data/tasks --final-output data/company_contacts.csv --config 1900comvn
