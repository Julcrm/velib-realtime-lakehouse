FROM python:3.10-slim-bookworm

# --- 1. Installation de uv ---
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# --- 2. Variables d'environnement ---
ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/*"
ENV DAGSTER_HOME=/opt/dagster/dagster_home
# Optimisations Python/Docker
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1

WORKDIR /opt/dagster/app

# --- 3. Install système + Java 17 + Curl ---
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- 4. Installation des dépendances Python via uv ---
COPY pyproject.toml uv.lock ./

# Installation synchronisée et verrouillée
RUN uv sync --frozen --no-dev

# --- 5. Téléchargement des Jars S3  ---
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# --- 6. Setup Dagster et Fichiers ---
RUN mkdir -p $DAGSTER_HOME

# Copie des configs et du code
COPY dagster.yaml workspace.yaml ./
COPY src/ ./src/

# --- 7. Finalisation ---
EXPOSE 4000

# On utilise "uv run" pour lancer la commande dans l'environnement virtuel
CMD sh -c "cp dagster.yaml $DAGSTER_HOME/dagster.yaml && \
           cp workspace.yaml $DAGSTER_HOME/workspace.yaml && \
           uv run dagster api grpc -h 0.0.0.0 -p 4000 -f src/definitions.py"