.PHONY: neat test clean

clean:
	@echo "🧼 Cleaning cache files..."
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete

neat:
	@echo "🧼 Cleaning code using autoflake, isort and black..."
	@autoflake --remove-all-unused-imports --recursive --in-place ingestion tests
	@isort --profile black ingestion tests
	@black ingestion tests
	@echo "✨ Code successful cleaned!"

test:
	@echo "🧪 Running suite tests..."
	-@pytest -s -vv --log-cli-level=INFO --cov=. --cov-report=term-missing tests
	@$(MAKE) clean
