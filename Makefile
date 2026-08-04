.PHONY: neat test

neat:
	@echo "🧼 Cleaning code using autoflake, isort and black..."
	autoflake --remove-all-unused-imports --recursive --in-place scripts ingestion
	isort --profile black scripts ingestion
	black scripts ingestion
	@echo "✨ Code successful cleaned!"

test:
	pytest -s -vv --log-cli-level=INFO --cov=. --cov-report=term-missing tests
