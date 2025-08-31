.PHONY: help install dev-install test lint format clean docker-build docker-up docker-down docker-logs crawl docker-crawl docker-crawl-1900 list-configs validate show-config docker-list-configs docker-validate docker-show-config docker-merge docker-merge-custom

# Default target
help:
	@echo "PCrawler - Available commands:"
	@echo ""
	@echo "Setup:"
	@echo "  install        Install production dependencies"
	@echo "  dev-install    Install development dependencies"
	@echo ""
	@echo "Development:"
	@echo "  test           Run tests"
	@echo "  lint           Run linting (flake8, mypy)"
	@echo "  format         Format code (black, isort)"
	@echo "  clean          Clean cache files"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build   Build Docker images"
	@echo "  docker-up      Start Docker services"
	@echo "  docker-down    Stop Docker services"
	@echo "  docker-logs    Show Docker logs"
	@echo "  docker-scale-1 Scale to 1 worker (conservative)"
	@echo "  docker-scale-2 Scale to 2 workers (recommended)"
	@echo "  docker-scale-3 Scale to 3 workers (aggressive)"
	@echo "  docker-scale-4 Scale to 4 workers (maximum)"
	@echo ""
	@echo "Crawling:"
	@echo "  crawl          Start crawling (default config)"
	@echo "  crawl-1900     Start crawling with 1900comvn config"
	@echo "  docker-crawl   Start crawling with Docker (default config)"
	@echo "  docker-crawl-1900 Start crawling with Docker (1900comvn config)"
	@echo ""
	@echo "Configuration:"
	@echo "  list-configs   List available configurations"
	@echo "  validate       Validate configuration"
	@echo "  show-config    Show configuration details"
	@echo "  docker-list-configs List configurations (Docker)"
	@echo "  docker-validate Validate configuration (Docker)"
	@echo "  docker-show-config Show configuration (Docker)"
	@echo ""
	@echo "Manual Operations:"
	@echo "  docker-merge   Manual merge with Docker (1900comvn)"
	@echo "  docker-merge-custom Manual merge with custom parameters"

# Installation
install:
	@echo "Installing production dependencies..."
	uv pip install -r requirements.txt

dev-install:
	@echo "Installing development dependencies..."
	uv pip install -e ".[dev]"
	pre-commit install

# Development
test:
	@echo "Running tests..."
	uv run pytest tests/ -v

lint:
	@echo "Running linting..."
	uv run flake8 app/ config/ tests/
	uv run mypy app/ config/

format:
	@echo "Formatting code..."
	uv run black app/ config/ tests/
	uv run isort app/ config/ tests/

clean:
	@echo "Cleaning cache files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/

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
docker-scale-workers:
	@echo "Scaling workers..."
	@read -p "Enter number of workers (default 4): " workers; \
	workers=$${workers:-4}; \
	docker-compose up --scale worker=$$workers -d

docker-scale-1:
	@echo "Scaling to 1 worker (conservative)..."
	docker-compose up --scale worker=1 -d

docker-scale-2:
	@echo "Scaling to 2 workers (recommended)..."
	docker-compose up --scale worker=2 -d

docker-scale-3:
	@echo "Scaling to 3 workers (aggressive)..."
	docker-compose up --scale worker=3 -d

docker-scale-4:
	@echo "Scaling to 4 workers (maximum for your CPU)..."
	docker-compose up --scale worker=4 -d

# Crawling
crawl:
	@echo "Starting crawling with default config..."
	uv run python -m app.main crawl

crawl-1900:
	@echo "Starting crawling with 1900comvn config..."
	uv run python -m app.main crawl --config 1900comvn

# Docker crawling
docker-crawl:
	@echo "Starting crawling with Docker (default config)..."
	docker-compose run --rm app python -m app.main crawl

docker-crawl-1900:
	@echo "Starting crawling with Docker (1900comvn config)..."
	docker-compose run --rm app python -m app.main crawl --config 1900comvn

# Configuration management
list-configs:
	@echo "Listing available configurations..."
	uv run python -m app.main list-configs

validate:
	@echo "Validating configuration..."
	uv run python -m app.main validate

show-config:
	@echo "Showing configuration details..."
	uv run python -m app.main show-config

# Docker configuration management
docker-list-configs:
	@echo "Listing available configurations (Docker)..."
	docker-compose run --rm app python -m app.main list-configs

docker-validate:
	@echo "Validating configuration (Docker)..."
	docker-compose run --rm app python -m app.main validate

docker-show-config:
	@echo "Showing configuration details (Docker)..."
	docker-compose run --rm app python -m app.main show-config

# Quick start
quick-start: docker-build docker-up
	@echo "Quick start completed!"
	@echo "Check logs with: make docker-logs"
	@echo "Stop with: make docker-down"

# Manual merge with Docker
docker-merge:
	@echo "Manual merge with Docker..."
	docker-compose run --rm app python merge_files.py --output-dir data/tasks --final-output data/company_contacts.csv --config 1900comvn

docker-merge-custom:
	@echo "Manual merge with custom parameters..."
	@read -p "Enter output directory: " output_dir; \
	read -p "Enter final output file: " final_output; \
	read -p "Enter config name (default): " config_name; \
	config_name=$${config_name:-default}; \
	docker-compose run --rm app python merge_files.py --output-dir $$output_dir --final-output $$final_output --config $$config_name
