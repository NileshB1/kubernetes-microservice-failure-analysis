

.DEFAULT_GOAL := help
.PHONY: help demo setup dev prod dash down clean build logs psql shell status \
        seed test test-all test-docker lint format check placeholders \
        download-dataset validate-dataset

COMPOSE := docker compose
COMPOSE_PROD := docker compose -f docker-compose.yml
PYTHON  := python

help: ## Show this help
	@echo ""
	@echo "  Distributed Analysis of Kubernetes Microservice Logs"
	@echo "  ---------------------------------------------------"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""


#getting started
demo: ## Zero-dependency demo - SQLite + dashboard, no Docker
	$(PYTHON) run_streamlit.py --local --browser

setup: ## Install runtime + development dependencies
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt
	@echo ""
	@echo "Next: cp .env.example .env, then set POSTGRES_PASSWORD and MINIO_SECRET_KEY."


#docker stack

dev: ## Docker quick-test - 1 worker, 100K rows
	$(COMPOSE) up --build -d
	$(COMPOSE) logs -f pipeline

prod: ## Docker production - 4 workers, 1M rows
	$(COMPOSE_PROD) up --build -d
	$(COMPOSE_PROD) logs -f pipeline

dash: ## Start only the dashboard (needs PostgreSQL running)
	$(COMPOSE) up -d dashboard
	@echo "Dashboard: http://localhost:8501"
	$(COMPOSE) logs -f dashboard
down: ## Stop all containers, keep data volumes
	$(COMPOSE) down

clean: ## Stop all, remove volumes, plots, and caches
	$(COMPOSE) down -v --remove-orphans
	rm -rf output/*.png htmlcov .coverage dashboard.db
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -prune -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -prune -exec rm -rf {} + 2>/dev/null || true

build: ## Build Docker images without starting them
	$(COMPOSE) build


# Quality gates 
test: ## Non-Spark tests
	bash scripts/run_tests.sh

test-all: ## Every test including Spark (needs a local JVM)
	$(PYTHON) -m pytest tests/
test-docker: ## Every test inside the built image
	$(COMPOSE) build pipeline
	$(COMPOSE) run --rm --no-deps pipeline /app/scripts/run_spark_tests.sh

lint: ## Lint and check formatting
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
format: ## Apply lint fixes and formatting
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

check: lint test ## Everything CI runs on a pull request


# Utilities

seed: ## Seed the database without launching the dashboard
	$(PYTHON) run_streamlit.py --seed-only

placeholders: ## Regenerate sample plots in output/
	$(PYTHON) scripts/generate_placeholders.py
logs: ## Follow the pipeline container's output
	$(COMPOSE) logs -f pipeline

psql: ## Open a PostgreSQL shell
	docker exec -it postgres psql -U $${POSTGRES_USER:-sparkuser} -d $${POSTGRES_DB:-microservice_analysis}
shell: ## Open a shell inside the pipeline container
	docker exec -it pipeline-runner bash

status: ## Show container status
	$(COMPOSE) ps

download-dataset: ## Download the Kaggle dataset into ./data
	$(PYTHON) scripts/download_kaggle_dataset.py --data-dir ./data
validate-dataset: ## Validate an already-downloaded dataset
	$(PYTHON) scripts/download_kaggle_dataset.py --validate-only --data-dir ./data
