# Dockerfile
FROM apache/airflow:3.0.1-python3.12

USER root

# Copier ton code
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/
COPY requirements.txt /opt/airflow/requirements.txt

# Installer toutes les dépendances
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Repasser en user airflow
USER airflow