FROM python:3.10-slim-bookworm

# --- 1. Installation de uv ---
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# --- 2. Variables d'environnement ---
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
# Important : Spark doit savoir où chercher ses JARs par défaut
ENV SPARK_JARS_DIR=/opt/spark/jars

WORKDIR /opt/dagster/app

# --- 3. Install système + Java 17 + Curl ---
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- 4. Installation des dépendances Python ---
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# --- 5. Téléchargement des JARs (S3 + KAFKA) ---
# On centralise tout ici pour éviter les "AnalysisException"
RUN mkdir -p /opt/spark/jars && \
    # Connecteur S3
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    # Connecteur Kafka (Spark SQL Kafka + Kafka Clients + Token Provider)
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# --- 6. Setup Dagster et Fichiers ---
RUN mkdir -p $DAGSTER_HOME
COPY dagster.yaml workspace.yaml $DAGSTER_HOME/
COPY dagster.yaml workspace.yaml ./
COPY src/ ./src/

EXPOSE 4000 8000
CMD ["uv", "run", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]