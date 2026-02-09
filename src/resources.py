"""
Définitions des ressources pour le pipeline Dagster.

Ce module définit les ressources utilisées par les assets, telles que la connexion MinIO (S3)
et la gestion des sessions Spark.
"""

from dagster import ConfigurableResource
from pyspark.sql import SparkSession, DataFrame
import boto3
import json
import os
from typing import Union


# --- RESSOURCE 1 : MINIO ---
class MinioResource(ConfigurableResource):
    """
    Ressource pour interagir avec un stockage d'objets MinIO (compatible S3).

    Attributs:
        endpoint_url: URL du serveur MinIO.
        access_key: Identifiant de la clé d'accès AWS.
        secret_key: Clé secrète AWS.
        bucket_name: Nom du bucket cible.
    """
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str

    def _get_client(self):
        """Crée et retourne un client boto3 S3."""
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def upload_json(self, key: str, data: Union[dict, bytes]) -> str:
        """
        Téléverse un objet JSON ou des octets vers MinIO.

        Args:
            key: La clé de l'objet S3 (chemin).
            data: Les données à téléverser (dict ou bytes).

        Retourne:
            Le chemin s3a vers l'objet téléversé.
        """
        client = self._get_client()
        if isinstance(data, dict):
            body = json.dumps(data)
        else:
            body = data  # Assure que c'est des bytes ou string

        client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType='application/json'
        )
        # Retourne le chemin s3a pour usage immédiat par Spark si besoin
        return f"s3a://{self.bucket_name}/{key}"

    def read_json(self, key: str) -> dict:
        """
        Lit un objet JSON depuis MinIO.

        Args:
            key: La clé de l'objet S3.

        Retourne:
            Les données JSON parsées sous forme de dictionnaire.
        """
        client = self._get_client()
        response = client.get_object(Bucket=self.bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)


# --- RESSOURCE 2 : SPARK ---
class SparkIO(ConfigurableResource):
    """
    Ressource pour la gestion de la Session Spark.

    Note: Idéalement, les clés d'accès devraient être passées ici comme attributs plutôt que via os.getenv
    pour permettre à Dagster d'injecter proprement les secrets via l'UI ou K8s.
    """

    def get_session(self, app_name: str = "DagsterSparkJob") -> SparkSession:
        """
        Initialise et retourne une SparkSession configurée pour S3A.

        Args:
            app_name: Nom de l'application Spark.

        Retourne:
            Une SparkSession configurée.
        """
        return (SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                # JARS : Toujours spécifier la version compatible. 3.3.4 est ok pour Spark 3.3+
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")

                # S3A CONFIG
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT_URL"))
                .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

                # PERFORMANCE LOCALE (Critique pour éviter les 200 tâches vides)
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.default.parallelism", "4")

                # OPTIONNEL : Pour éviter que Spark ne crashe sur des fichiers _SUCCESS
                .config("spark.sql.files.ignoreCorruptFiles", "true")
                .getOrCreate())

    def read_data(self, spark: SparkSession, path_or_paths: Union[str, list], format: str = "json",
                  base_path: str = None) -> DataFrame:
        """
        Lit intelligemment des données depuis S3.

        Args:
            spark: SparkSession active.
            path_or_paths: Un chemin unique (str) OU une liste de chemins (list) pour le filtrage manuel.
            format: Format des données (json, parquet, etc.).
            base_path: CRITIQUE si on passe une liste de chemins partitionnés.
                       Indique à Spark où commence la racine pour reconstruire les partitions.

        Retourne:
            Un DataFrame Spark.
        """
        reader = spark.read

        # Si un base_path est fourni (ex: s3a://bronze/velib), on l'active.
        if base_path:
            reader = reader.option("basePath", base_path)

        # Gestion du format
        if format == "json":
            df = reader.json(path_or_paths)
        elif format == "parquet":
            df = reader.parquet(path_or_paths)
        else:
            df = reader.format(format).load(path_or_paths)

        return df