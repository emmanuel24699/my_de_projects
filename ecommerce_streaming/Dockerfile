FROM bitnami/spark:3.5.3
COPY config/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/bitnami/spark/jars/postgresql-42.7.4.jar