FROM python:3.10-slim-bookworm

# 1. Variables d'env globales
ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/*"
ENV DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /opt/dagster/app

# 2. Install système + Java 17 (Plus stable pour Spark 3.x) + Curl
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME pour que PySpark trouve Java
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 3. Requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. TÉLÉCHARGEMENT DES JARS S3
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 5. Setup Dagster Home
RUN mkdir -p $DAGSTER_HOME && \
    touch $DAGSTER_HOME/dagster.yaml

# 6. COPIE DU CODE
COPY src/ ./src/

# 7. EXPOSER LE PORT ET COMMANDES PAR DÉFAUT
EXPOSE 3000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "src/definitions.py"]