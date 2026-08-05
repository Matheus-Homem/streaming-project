.PHONY: neat test clean

clean:
	@echo "🧼 Cleaning cache files..."
	@find . -type f -name "*.pyc" ! -path "./.venv/*" -delete
	@find . -type d -name "__pycache__" ! -path "./.venv/*" -delete

neat:
	@echo "🧼 Cleaning code using autoflake, isort and black..."
	@autoflake --remove-all-unused-imports --recursive --in-place shared ingestion tests
	@isort --profile black shared ingestion tests
	@black shared ingestion tests
	@echo "✨ Code successful cleaned!"

test:
	@echo "🧪 Running suite tests..."
	-@pytest -s -vv --log-cli-level=INFO --cov=. --cov-report=term-missing tests
	@$(MAKE) clean
