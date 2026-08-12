
#application image 

ARG PYTHON_IMAGE=python:3.11-slim-bookworm


# stage 1 

FROM ${PYTHON_IMAGE} AS deps


ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN_BASE=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /jars; \
    download() { \
        url="$1"; file="/jars/$(basename "$url")"; \
        curl -fsSL "$url" -o "$file"; \
        curl -fsSL "${url}.sha1" -o "${file}.sha1"; \
        echo "$(cat "${file}.sha1")  ${file}" | sha1sum -c -; \
        rm -f "${file}.sha1"; \
    }; \
    download "${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    
    download "${MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    
    
    download "${MAVEN_BASE}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# Python dependencies into a relocatable prefix, copied into the runtime
# stage
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --prefix=/python-deps -r /tmp/requirements.txt



# step 2
FROM ${PYTHON_IMAGE} AS runtime

LABEL org.opencontainers.image.title="k8s-microservice-failure-analysis" \
      org.opencontainers.image.description="Distributed analysis of Kubernetes microservice logs" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.licenses="MIT"

# Spark needs a JVM; procps supplies the `ps` its launcher scripts call
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl ca-certificates procps postgresql-client openjdk-17-jre-headless; \
    rm -rf /var/lib/apt/lists/*

COPY --from=deps /python-deps /usr/local

#spark ships inside the pyspark wheel. /opt/spark is symlinked to it 
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
RUN ln -sfn "${SPARK_HOME}" /opt/spark

COPY --from=deps /jars/*.jar ${SPARK_HOME}/jars/

# PYSPARK_PYTHON must name the same interpreter on driver and executors
ENV PYTHONPATH="/app" \
    PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}" \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    SPARK_WORKER_DIR=/tmp/spark-work \
    SPARK_LOCAL_DIRS=/tmp/spark-local \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1


ARG APP_UID=10001
RUN set -eux; \
    groupadd --gid ${APP_UID} appuser; \
    useradd --uid ${APP_UID} --gid ${APP_UID} --create-home --shell /bin/bash appuser; \
    mkdir -p /app /data /output /tmp/spark-events /tmp/spark-work /tmp/spark-local; \
    chown -R appuser:appuser /output /data /tmp/spark-events /tmp/spark-work /tmp/spark-local

WORKDIR /app

COPY --chown=root:root modules/ /app/modules/
COPY --chown=root:root config/ /app/config/
COPY --chown=root:root scripts/ /app/scripts/
COPY --chown=root:root sql/ /app/sql/
COPY --chown=root:root run_streamlit.py /app/run_streamlit.py
COPY --chown=root:root .streamlit/ /app/.streamlit/


RUN sed -i 's/\r$//' /app/scripts/*.sh \
    && chmod +x /app/scripts/*.sh

USER appuser

EXPOSE 8501

#reports unhealthy if Streamlit stops serving
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
