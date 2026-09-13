.PHONY: help setup generate-data run-bronze run-silver run-gold run-pipeline \
        test lint format dashboard lineage-ui airflow-ui spark-ui clean

PYTHON := python3
SPARK_MASTER := spark://localhost:7077

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

# ── Environment ──────────────────────────────────────────────────────────────

setup: ## Install Python deps + copy .env
	@cp -n .env.example .env || true
	pip install -r requirements.txt
	@echo "✓ Setup complete. Edit .env if needed."

infra-up: ## Start all Docker services
	docker-compose up -d
	@echo "✓ Services starting... (run 'make status' to check)"

infra-down: ## Stop all Docker services
	docker-compose down

infra-clean: ## Stop services and remove volumes
	docker-compose down -v

status: ## Show service health
	docker-compose ps

# ── Data Generation ───────────────────────────────────────────────────────────

generate-data: ## Generate 90-day synthetic wearable dataset (10 users)
	$(PYTHON) data/generate_data.py --users 10 --days 90 --output data/raw
	@echo "✓ Synthetic data written to data/raw/"

# ── Pipeline Stages ───────────────────────────────────────────────────────────

run-bronze: ## Ingest raw data → Bronze Iceberg tables
	docker exec fitlake-spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/spark/jobs/bronze_ingestion.py

run-silver: ## Bronze → Silver (clean, deduplicate, validate)
	docker exec fitlake-spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/spark/jobs/silver_transformation.py

run-gold: ## Silver → Gold (recovery scores, strain, sleep analytics)
	docker exec fitlake-spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/spark/jobs/gold_aggregation.py

run-pipeline: generate-data run-bronze run-silver run-gold ## Run full E2E pipeline
	@echo "✓ Full pipeline complete!"

run-quality: ## Run Great Expectations data quality checks
	$(PYTHON) quality/run_checks.py

# ── Local Dev (no Docker) ─────────────────────────────────────────────────────

run-local: generate-data ## Run pipeline locally (no Docker, uses local files)
	$(PYTHON) pipeline/run_local.py
	@echo "✓ Local pipeline complete! Run 'make dashboard-local' to view insights."

# ── UI / Dashboards ───────────────────────────────────────────────────────────

dashboard-local: ## Open Streamlit dashboard (local mode, no Docker)
	streamlit run analytics/dashboard.py

dashboard: ## Open Streamlit dashboard (Docker)
	@open http://localhost:8501

spark-ui: ## Open Spark Master UI
	@open http://localhost:8080

airflow-ui: ## Open Airflow UI (admin/admin)
	@open http://localhost:8081

lineage-ui: ## Open Marquez data lineage UI
	@open http://localhost:3000

minio-ui: ## Open MinIO console (minioadmin/minioadmin)
	@open http://localhost:9001

# ── Quality & Testing ─────────────────────────────────────────────────────────

test: ## Run test suite with coverage
	pytest tests/ -v --cov=pipeline --cov=spark --cov-report=term-missing

test-fast: ## Run tests excluding slow integration tests
	pytest tests/ -v -m "not integration" --cov=pipeline

lint: ## Run ruff linter
	ruff check pipeline/ spark/ quality/ analytics/ tests/ data/

format: ## Auto-format with black
	black pipeline/ spark/ quality/ analytics/ tests/ data/

typecheck: ## Run mypy type checks
	mypy pipeline/ spark/ --ignore-missing-imports

# ── Iceberg Operations ────────────────────────────────────────────────────────

iceberg-time-travel: ## Demo: query data as of 30 days ago (Iceberg time travel)
	$(PYTHON) -c "\
import duckdb; \
conn = duckdb.connect(); \
conn.execute('INSTALL iceberg; LOAD iceberg;'); \
print('Iceberg time-travel demo — run: python sql/time_travel_demo.py')"

iceberg-snapshot: ## List Iceberg snapshots (version history)
	$(PYTHON) sql/list_snapshots.py

# ── Infra (AWS — optional) ────────────────────────────────────────────────────

tf-init: ## Terraform init (real AWS deployment)
	cd infrastructure/terraform && terraform init

tf-plan: ## Terraform plan
	cd infrastructure/terraform && terraform plan

tf-apply: ## Terraform apply
	cd infrastructure/terraform && terraform apply

clean: ## Remove generated data and cache
	rm -rf data/raw/ data/output/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	@echo "✓ Cleaned."
