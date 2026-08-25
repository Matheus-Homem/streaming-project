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

COMPOSE_FILES := -f infra/docker/docker-compose.yml
ifeq ($(MODE),dev)
COMPOSE_FILES += -f infra/docker/docker-compose.dev.yml
endif

# MODE=dev shrinks Kafka to 1 controller + 1 broker (see infra/docker/docker-compose.dev.yml).
# Default (no MODE, or MODE=full) keeps the full 3+3 topology.
kafka-up:
	@echo "🚀 Starting Kafka docker environment (MODE=$(or $(MODE),full))..."
	@docker compose $(COMPOSE_FILES) up -d

kafka-down:
	@echo "🛑 Stopping Kafka docker environment (MODE=$(or $(MODE),full))..."
	@docker compose $(COMPOSE_FILES) down --remove-orphans
