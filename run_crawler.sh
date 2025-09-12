#!/bin/bash

# PCrawler - Professional Web Crawler with Phase Selection
# This script forces users to select a phase before running the crawler

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show help
show_help() {
    echo "PCrawler - Professional Web Crawler with Phase Selection"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --phase PHASE     Start from specific phase (1,2,3,4,5,auto)"
    echo "  --force-restart   Force restart from Phase 1"
    echo "  --config CONFIG   Config name (default: 1900comvn)"
    echo "  --scale N         Scale workers to N instances (default: 1)"
    echo "  --logs           Show logs from running containers"
    echo "  --help           Show this help message"
    echo ""
    echo "Phases:"
    echo "  1    - Crawl links for all industries"
    echo "  2    - Crawl detail pages from links"
    echo "  3    - Crawl contact pages from company details"
    echo "  4    - Extract company details and emails"
    echo "  5    - Export final CSV"
    echo "  auto - Auto-detect starting phase (recommended)"
    echo ""
    echo "Examples:"
    echo "  $0 --phase auto                    # Auto-detect and start from appropriate phase"
    echo "  $0 --phase 2                       # Start from Phase 2 (detail crawling)"
    echo "  $0 --phase 1 --force-restart       # Force restart from Phase 1"
    echo "  $0 --phase auto --config myconfig  # Use custom config with auto phase"
    echo "  $0 --phase 3 --scale 5             # Start Phase 3 with 5 workers"
    echo "  $0 --logs                          # Show logs from running containers"
}

# Function to validate phase
validate_phase() {
    local phase=$1
    case $phase in
        1|2|3|4|5|auto)
            return 0
            ;;
        *)
            print_error "Invalid phase: $phase"
            print_error "Valid phases: 1, 2, 3, 4, 5, auto"
            return 1
            ;;
    esac
}

# Function to run crawler with phase selection
run_crawler() {
    local phase=$1
    local force_restart=$2
    local config=$3
    local scale=$4
    
    print_info "Starting PCrawler with phase selection..."
    print_info "Phase: $phase"
    print_info "Config: $config"
    print_info "Workers: $scale"
    if [ "$force_restart" = "true" ]; then
        print_warning "Force restart enabled - will start from Phase 1"
    fi
    
    # Build command using docker-compose run with real-time output
    local cmd=""
    if [ "$force_restart" = "true" ]; then
        cmd="docker-compose run --rm --no-deps crawler_app python -m app.main crawl --phase 1 --force-restart --config $config"
    else
        cmd="docker-compose run --rm --no-deps crawler_app python -m app.main crawl --phase $phase --config $config"
    fi
    
    print_info "Executing: $cmd"
    echo ""
    
    # Execute command with real-time output and show logs
    print_info "Starting crawler with real-time logs..."
    echo ""
    
    # Run the command and show output in real-time
    eval $cmd
    
    # After command completes, show recent logs
    echo ""
    print_info "Crawler completed. Recent logs:"
    docker-compose logs --tail=50
}

# Function to show current status
show_status() {
    print_info "Checking current crawler status..."
    
    # Check if containers are running
    if docker-compose ps | grep -q "Up"; then
        print_success "Docker containers are running"
    else
        print_warning "Docker containers are not running"
    fi
    
    # Check data directory
    if [ -d "data" ]; then
        local checkpoint_count=$(find data -name "checkpoint_*.json" 2>/dev/null | wc -l)
        local csv_exists=""
        if [ -f "data/company_contacts.csv" ]; then
            csv_exists=" (CSV exists)"
        fi
        print_info "Data directory: $checkpoint_count checkpoint files$csv_exists"
    else
        print_warning "Data directory not found"
    fi
}

# Main script logic
main() {
    local phase=""
    local force_restart="false"
    local config="1900comvn"
    local scale="1"
    local show_help_flag="false"
    local show_logs_flag="false"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --phase)
                phase="$2"
                shift 2
                ;;
            --force-restart)
                force_restart="true"
                shift
                ;;
            --config)
                config="$2"
                shift 2
                ;;
            --scale)
                scale="$2"
                shift 2
                ;;
            --logs)
                show_logs_flag="true"
                shift
                ;;
            --help|-h)
                show_help_flag="true"
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Show help if requested
    if [ "$show_help_flag" = "true" ]; then
        show_help
        exit 0
    fi
    
    # Show logs if requested
    if [ "$show_logs_flag" = "true" ]; then
        print_info "Showing logs from running containers..."
        docker-compose logs -f
        exit 0
    fi
    
    # If no phase specified, prompt user
    if [ -z "$phase" ]; then
        echo "PCrawler - Professional Web Crawler with Phase Selection"
        echo ""
        show_status
        echo ""
        echo "Please select a phase to start from:"
        echo "  1) Phase 1 - Crawl links for all industries"
        echo "  2) Phase 2 - Crawl detail pages from links"
        echo "  3) Phase 3 - Crawl contact pages from company details"
        echo "  4) Phase 4 - Extract company details and emails"
        echo "  5) Phase 5 - Export final CSV"
        echo "  a) Auto-detect starting phase (recommended)"
        echo "  f) Force restart from Phase 1"
        echo "  h) Show help"
        echo "  q) Quit"
        echo ""
        read -p "Enter your choice (1-5, a, f, h, q): " choice
        
        case $choice in
            1) phase="1" ;;
            2) phase="2" ;;
            3) phase="3" ;;
            4) phase="4" ;;
            5) phase="5" ;;
            a|A) phase="auto" ;;
            f|F) phase="1"; force_restart="true" ;;
            h|H) show_help; exit 0 ;;
            q|Q) print_info "Exiting..."; exit 0 ;;
            *) print_error "Invalid choice: $choice"; exit 1 ;;
        esac
    fi
    
    # Validate phase
    if ! validate_phase "$phase"; then
        exit 1
    fi
    
    # Scale workers if needed
    if [ "$scale" != "1" ]; then
        print_info "Scaling workers to $scale instances..."
        docker-compose up -d --scale worker=$scale
        print_info "Waiting 5 seconds for workers to start..."
        sleep 5
    fi
    
    # Run crawler
    run_crawler "$phase" "$force_restart" "$config" "$scale"
}

# Run main function
main "$@"
