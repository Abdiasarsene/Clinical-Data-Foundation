.PHONY: run_ingestion format cyclo code test

# Dossier du test
TEST_DIR = tests
PIPELINE_DIR = pipeline
AIRFLOW_DIR = dags
SRC_DIR = src
OBSERVABILITY_DIR = observability

# Defaut Pipeline
default: format cyclo code mypy
	@echo "Default Pipeline Done"

# Pipeline Complet d'entraînement
run_ingestion:
	@echo "Lancement du Pipeline Global"
	@python runner.py

# Linting + Formatage
ruff:
	@echo "Linting + Format"
	@ruff check . --fix

# Cyclomatic Analysis
radon:
	@echo CC Analysis
	@radon mi $(PIPELINE_DIR)/ $(SRC_DIR)/ $(OBSERVABILITY_DIR)/ $(AIRFLOW_DIR)/ $(TEST_DIR)/ -s

# Code Analysis
bandit:
	@echo "Code Analysis"
	@bandit -r $(PIPELINE_DIR)/ $(SRC_DIR)/ $(OBSERVABILITY_DIR)/ $(AIRFLOW_DIR)/ $(TEST_DIR)/ -ll

# Mypy
mypy:
	@echo "Mypy's running"
	@mypy --config mypy.ini

# Test
test:
	@echo "Test Unitaire"
	@pytest

# Docker services status
docker:
	@echo "Docker Services status"
	@python check_services_status.py