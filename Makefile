# Makefile for TimescaleDB CRUD System

.PHONY: help install test test-unit test-integration test-all coverage clean docker-up docker-down docker-logs

# Default target
help:
	@echo "Available targets:"
	@echo "  install          - Install dependencies"
	@echo "  test            - Run all tests"
	@echo "  test-unit       - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-all        - Run all tests with coverage"
	@echo "  coverage        - Run tests with coverage report"
	@echo "  docker-up       - Start TimescaleDB with Docker Compose"
	@echo "  docker-down     - Stop Docker Compose services"
	@echo "  docker-logs     - Show Docker Compose logs"
	@echo "  clean           - Clean up test artifacts"

# Install dependencies
install:
	uv sync --group dev

# Docker operations
docker-up:
	docker compose up -d timescaledb
	@echo "Waiting for TimescaleDB to be ready..."
	@timeout 30 bash -c 'until docker compose exec timescaledb pg_isready -U tsdb_user; do sleep 1; done'
	@echo "TimescaleDB is ready!"

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f timescaledb

# Test targets
test-unit:
	python tests/test_runner.py unit

test-integration: docker-up
	python tests/test_runner.py integration

test: test-unit test-integration

test-all: docker-up
	python tests/test_runner.py all --coverage

coverage: docker-up
	python tests/test_runner.py all --coverage
	@echo "Coverage report generated in htmlcov/"

# Alternative pytest commands
pytest-unit:
	pytest tests/unit/ -v

pytest-integration: docker-up
	TEST_DATABASE_URL="postgresql://test_user:test_password@localhost:5432/tsdb" pytest tests/integration/ -v

pytest-all: docker-up
	TEST_DATABASE_URL="postgresql://test_user:test_password@localhost:5432/tsdb" pytest tests/ -v --cov=tsdb --cov-report=html

# Clean up
clean:
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Development helpers
dev-setup: install docker-up
	@echo "Development environment ready!"
	@echo "TimescaleDB: postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb"
	@echo "pgAdmin: http://localhost:8080 (admin@tsdb.local / admin)"

# Quick test cycle
quick-test:
	python tests/test_runner.py unit

# Full test cycle with cleanup
full-test: clean docker-up test-all docker-down
