"""
Définitions des ressources pour le pipeline Dagster.
Module complet : MinIO + Spark.
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
    """
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str

    def get_client(self):
        """
        Crée et retourne un client boto3 S3.
        Méthode publique utilisée par les assets de maintenance.
        """
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def upload_json(self, key: str, data: Union[dict, bytes]) -> str:
        """Téléverse un objet JSON ou des octets vers MinIO."""
        client = self.get_client()

        if isinstance(data, dict):
            body = json.dumps(data)
        else:
            body = data

        client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType='application/json'
        )
        return f"s3a://{self.bucket_name}/{key}"

    def read_json(self, key: str) -> dict:
        """Lit un objet JSON depuis MinIO."""
        client = self.get_client()
        response = client.get_object(Bucket=self.bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)


# --- RESSOURCE 2 : SPARK ---
class SparkIO(ConfigurableResource):
    """
    Ressource pour la gestion de la Session Spark.
    """

    def get_session(self, app_name: str = "DagsterSparkJob") -> SparkSession:
        """Initialise et retourne une SparkSession configurée pour S3A."""
        return (SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT_URL"))
                .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.default.parallelism", "4")
                .config("spark.sql.files.ignoreCorruptFiles", "true")
                .getOrCreate())

    def read_data(self, spark: SparkSession, path_or_paths: Union[str, list], format: str = "json",
                  base_path: str = None) -> DataFrame:
        """Lit intelligemment des données depuis S3."""
        reader = spark.read

        if base_path:
            reader = reader.option("basePath", base_path)

        if format == "json":
            df = reader.json(path_or_paths)
        elif format == "parquet":
            df = reader.parquet(path_or_paths)
        else:
            df = reader.format(format).load(path_or_paths)

        return df