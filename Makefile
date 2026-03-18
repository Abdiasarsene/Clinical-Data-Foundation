.PHONY: run_ingestion format cyclo code api

# Dossier du test
TEST_DIR = test
ORCHESTRATION_DIR = orchestration
PIPELINE_DIR = orchestration/pipeline
AIRFLOW_DIR = orchestration/airflow
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
format:
	@echo "Linting + Format"
	@ruff check . --fix

# Cyclomatic Analysis
cyclo:
	@echo CC Analysis
	@radon mi $(ORCHESTRATION_DIR)/ $(SRC_DIR)/ $(OBSERVABILITY_DIR)/ -s

# Code Analysis
code:
	@echo "Code Analysis"
	@bandit -r $(ORCHESTRATION_DIR)/ $(SRC_DIR)/ $(OBSERVABILITY_DIR)/ -ll

# Mypy
mypy:
	@echo "Mypy's running"
	@mypy --config mypy.ini