.PHONY: help docker-build docker-up docker-down docker-logs crawl docker-crawl docker-crawl-1900 docker-scale-1 docker-scale-2 docker-scale-4 docker-scale-8 docker-merge

# Default target
help:
	@echo "PCrawler - Optimized for Intel i7-12700 (20 cores) + 32GB RAM:"
	@echo "CPU Usage: 50% (10/20 cores) - Balanced performance/risk"
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
	@echo "  docker-scale-1 Safe mode (1 worker) - Low risk, slower"
	@echo "  docker-scale-2 Balanced mode (2 workers) - Medium risk/speed"
	@echo "  docker-scale-4 Fast mode (4 workers) - High performance, medium risk"
	@echo "  docker-scale-8 Turbo mode (8 workers) - Max performance, higher risk"
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

# Docker scaling - tối ưu cho CPU 20 cores (50% usage)
docker-scale-1:
	@echo "Safe mode (1 worker) - Low risk, slower"
	docker-compose up --scale worker=1 -d

docker-scale-2:
	@echo "Balanced mode (2 workers) - Medium risk/speed"
	docker-compose up --scale worker=2 -d

docker-scale-4:
	@echo "Fast mode (4 workers) - High performance, medium risk"
	docker-compose up --scale worker=4 -d

docker-scale-8:
	@echo "Turbo mode (8 workers) - Max performance, higher risk"
	docker-compose up --scale worker=8 -d

# Docker crawling
docker-crawl:
	@echo "Starting crawling (Docker)..."
	docker-compose run --rm app python -m app.main crawl --config 1900comvn

# Manual merge
docker-merge:
	@echo "Merging CSV files..."
	docker-compose run --rm app python merge_files.py --output-dir data/tasks --final-output data/company_contacts.csv --config 1900comvn
