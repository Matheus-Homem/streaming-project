export PYTHONDONTWRITEBYTECODE := 1

.PHONY: neat test clean ingestion-default kafka-up kafka-down

clean:
	@echo "🧼 Cleaning cache files..."
	@find . -type f -name "*.pyc" ! -path "./.venv/*" -delete
	@find . -type d -name "__pycache__" ! -path "./.venv/*" -delete

neat:
	@echo "🧼 Cleaning code using autoflake, isort and black..."
	@autoflake --remove-all-unused-imports --recursive --in-place ingestion flink shared tests
	@isort --profile black ingestion flink shared tests
	@black ingestion flink shared tests
	@echo "✨ Code successful cleaned!"
	@$(MAKE) clean

test:
	@echo "🧪 Running suite tests..."
	-@python -m pytest -p no:cacheprovider -s -vv --log-cli-level=INFO --cov=ingestion --cov=flink --cov-report=term-missing tests

ingestion-default:
	@python -m ingestion.app --source github

kafka-up:
	@echo "🚀 Starting Kafka docker environment..."
	@docker compose -f infra/docker/docker-compose.yml up -d

kafka-down:
	@echo "🛑 Stopping Kafka docker environment..."
	@docker compose -f infra/docker/docker-compose.yml down
