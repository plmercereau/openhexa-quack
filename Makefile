.PHONY: help build up down restart logs clean install graphql test

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install Python dependencies locally
	pip install -e .[dev]

build: ## Build Docker images
	docker compose build

up: ## Start services
	docker compose up -d
	@echo ""
	@echo "✓ Services started!"
	@echo "  Superset: http://localhost:8088 (admin/admin)"
	@echo ""

down: ## Stop services
	docker compose down

restart: down up ## Restart services

logs: ## Show logs
	docker compose logs -f superset

clean: ## Clean up containers, volumes, and cache
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

shell: ## Open shell in superset container
	docker compose exec superset bash

test: ## Run tests
	pytest

format: ## Format code with black
	black duckdb_openhexa

lint: ## Lint code
	black --check duckdb_openhexa
	pylint duckdb_openhexa || true

