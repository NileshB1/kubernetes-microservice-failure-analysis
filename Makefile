# ============================================================
# Makefile — Distributed Analysis of Kubernetes Microservice Logs
# ============================================================
# Primary Targets:
#   make dev         Quick-test Docker: 1 worker, 100K rows
#   make prod        Production Docker: 4 workers, 1M rows
#   make dash        Start dashboard via Docker at http://localhost:8501
#   make dash-local  Start dashboard locally (no Docker, seeds sample data)
#   make seed        Seed PostgreSQL with sample data (no Docker)
#   make test        Run pytest unit tests (requires local Spark + Java)
#   make down        Stop all Docker containers, keep volumes
#   make clean       Stop all + remove volumes, output, __pycache__
#
# Helpers:
#   make build       Build Docker images without starting
#   make logs        Follow pipeline container output
#   make psql        Open PostgreSQL interactive shell
#   make shell       Open bash in pipeline container
#   make status      Show running containers
#   make test-ci     Run 127 non-Spark tests (CI-friendly, no deps)
#   make test-docker Run full test suite in Docker (Spark + non-Spark)
# ============================================================

.PHONY: dev prod test down clean build logs psql shell status dash dash-local seed test-ci test-docker

COMPOSE_DEV  := docker compose
COMPOSE_PROD := docker compose -f docker-compose.yml
PIPELINE     := pipeline-runner

# ----------------------------------------------------------
# Primary Targets
# ----------------------------------------------------------

dev:
	@echo "=== Starting QUICK-TEST mode (1 worker, 100K rows) ==="
	$(COMPOSE_DEV) up --build -d
	@echo "=== Watching pipeline logs (Ctrl+C to stop watching) ==="
	$(COMPOSE_DEV) logs -f pipeline

prod:
	@echo "=== Starting PRODUCTION mode (4 workers, 1M rows) ==="
	$(COMPOSE_PROD) up --build -d
	@echo "=== Watching pipeline logs (Ctrl+C to stop watching) ==="
	$(COMPOSE_PROD) logs -f pipeline

test:
	@echo "=== Running pytest ==="
	cd "$(CURDIR)" && python -m pytest tests/ -v --tb=short

down:
	@echo "=== Stopping all services ==="
	$(COMPOSE_DEV) down
	@echo "=== Done ==="

clean:
	@echo "=== Stopping all services and removing volumes ==="
	$(COMPOSE_DEV) down -v --remove-orphans
	@echo "=== Removing output files ==="
	rm -rf output/*.png
	@echo "=== Removing Python cache ==="
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@echo "=== Clean complete ==="

# ----------------------------------------------------------
# Helper Targets
# ----------------------------------------------------------

build:
	$(COMPOSE_DEV) build

logs:
	$(COMPOSE_DEV) logs -f pipeline

psql:
	@echo "=== Connecting to PostgreSQL ==="
	docker exec -it postgres psql -U sparkuser -d microservice_analysis

shell:
	@echo "=== Opening shell in pipeline container ==="
	docker exec -it $(PIPELINE) bash

status:
	@echo "=== Container Status ==="
	$(COMPOSE_DEV) ps

# ----------------------------------------------------------
# Shortcuts
# ----------------------------------------------------------

dash:
	@echo "=== Starting Streamlit Dashboard (Docker) ==="
	$(COMPOSE_DEV) up -d dashboard
	@echo ""
	@echo "Dashboard: http://localhost:8501"
	$(COMPOSE_DEV) logs -f dashboard

dash-local:
	@echo "=== Starting Streamlit Dashboard (local PostgreSQL) ==="
	python run_streamlit.py --browser

dash-sqlite:
	@echo "=== Starting Streamlit Dashboard (SQLite — zero deps) ==="
	python run_streamlit.py --local --browser

seed:
	@echo "=== Seeding PostgreSQL with sample data ==="
	python run_streamlit.py --seed-only

placeholders:
	@echo "=== Generating placeholder plots ==="
	cd "$(CURDIR)" && python scripts/generate_placeholders.py

download-dataset:
	@echo "=== Downloading Kaggle dataset ==="
	cd "$(CURDIR)" && python scripts/download_kaggle_dataset.py --data-dir ./data

validate-dataset:
	@echo "=== Validating existing dataset ==="
	cd "$(CURDIR)" && python scripts/download_kaggle_dataset.py --validate-only --data-dir ./data

test-ci:
	@echo "=== Running non-Spark test suite (CI mode) ==="
	bash scripts/run_tests.sh

test-docker:
	@echo "=== Building Docker image for full test suite ==="
	docker compose build pipeline
	@echo ""
	@echo "=== Running full test suite (Spark + non-Spark) in Docker ==="
	docker compose run --rm --no-deps pipeline /app/scripts/run_spark_tests.sh

d: dev
p: prod
t: test
