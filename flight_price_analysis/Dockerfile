FROM apache/airflow:2.10.2

USER root

# Install OS-level packages: MySQL and PostgreSQL clients
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-mysql-client \
    postgresql-client \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
