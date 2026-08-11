# ============================================================
# Base image with Spark 3.5 + Python 3.11 + Jupyter
# ============================================================
FROM bitnami/spark:3.5.3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    unzip \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Add Hadoop AWS connector for S3/MinIO
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -P /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -P /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
    -P /opt/bitnami/spark/jars/

# Create app directories
RUN mkdir -p /app /data /output /tmp/spark-events

# Copy application code
COPY modules/ /app/modules/
COPY config/ /app/config/
COPY scripts/ /app/scripts/
COPY sql/ /app/sql/
COPY tests/ /app/tests/
COPY pytest.ini /app/pytest.ini

# Make scripts executable
RUN chmod +x /app/scripts/*.sh

WORKDIR /app
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Default entrypoint
ENTRYPOINT ["/app/scripts/entrypoint.sh"]
