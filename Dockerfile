FROM apache/airflow:3.0.1-python3.12

USER root

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/opt/airflow"

# On reste dans le dossier Airflow
WORKDIR /opt/airflow

# Installer curl (optionnel)
RUN apt-get update \
    && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

# Copier uniquement ce qu'il faut
COPY src/ src/
COPY observability/ observability/
COPY pipelines/ pipelines/
COPY dags/ dags/

# Copier Poetry
COPY pyproject.toml poetry.lock ./

# Installer Poetry
RUN pip install poetry

# Installer dépendances
RUN poetry config virtualenvs.create false \
    && poetry install --without dev --no-root --no-interaction --no-ansi

USER airflow