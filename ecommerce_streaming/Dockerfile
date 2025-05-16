FROM bitnami/spark:3.5.3

# Switch to root to perform installations and permission changes
USER root

# Fix for missing apt partial list directory, install curl, and clean up
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Copy Python requirements file and install Python dependencies
COPY config/requirements.txt /opt/bitnami/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt

# Create necessary directories for data, logs, and checkpoints
RUN mkdir -p /data/input /data/archive /data/checkpoints /logs && \
    chown -R 1000:1000 /data /logs && \
    chmod -R 775 /data /logs

# Download PostgreSQL JDBC driver directly into Spark jars folder
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# OPTIONAL: If you want to copy from local instead of downloading, uncomment this:
# COPY jars/postgresql-42.7.4.jar /opt/bitnami/spark/jars/postgresql-42.7.4.jar

# Create spark user and fix permissions for Spark directories
RUN groupadd -g 1000 spark && \
    useradd -u 1000 -g spark -m spark && \
    mkdir -p /opt/bitnami/spark/tmp && \
    chown -R 1000:1000 /opt/bitnami/spark /opt/bitnami/spark/tmp

# Switch to spark user for runtime
USER spark


